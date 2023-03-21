package chandy_lamport

import (
	"log"
)

type void struct{}

var voidmember void

type ServerSnapShot struct {
	tokens   int
	peers    map[string]void
	messages map[string][]*SnapshotMessage
}

func NewServerSnapShot() *ServerSnapShot {
	snap := ServerSnapShot{
		0,
		make(map[string]void),
		make(map[string][]*SnapshotMessage),
	}

	return &snap
}

// The main participant of the distributed snapshot protocol.
// Servers exchange token messages and marker messages among each other.
// Token messages represent the transfer of tokens from one server to another.
// Marker messages represent the progress of the snapshot process. The bulk of
// the distributed protocol is implemented in `HandlePacket` and `StartSnapshot`.
type Server struct {
	Id            string
	Tokens        int
	sim           *Simulator
	outboundLinks map[string]*Link // key = link.dest
	inboundLinks  map[string]*Link // key = link.src
	// TODO: ADD MORE FIELDS HERE
	snapShots *SyncMap
}

// A unidirectional communication channel between two servers
// Each link contains an event queue (as opposed to a packet queue)
type Link struct {
	src    string
	dest   string
	events *Queue
}

func NewServer(id string, tokens int, sim *Simulator) *Server {
	return &Server{
		id,
		tokens,
		sim,
		make(map[string]*Link),
		make(map[string]*Link),
		NewSyncMap(),
	}
}

// Add a unidirectional link to the destination server
func (server *Server) AddOutboundLink(dest *Server) {
	if server == dest {
		return
	}
	l := Link{server.Id, dest.Id, NewQueue()}
	server.outboundLinks[dest.Id] = &l
	dest.inboundLinks[server.Id] = &l
}

// Send a message on all of the server's outbound links
func (server *Server) SendToNeighbors(message interface{}) {
	for _, serverId := range getSortedKeys(server.outboundLinks) {
		link := server.outboundLinks[serverId]
		server.sim.logger.RecordEvent(
			server,
			SentMessageEvent{server.Id, link.dest, message})
		link.events.Push(SendMessageEvent{
			server.Id,
			link.dest,
			message,
			server.sim.GetReceiveTime()})
	}
}

// Send a number of tokens to a neighbor attached to this server
func (server *Server) SendTokens(numTokens int, dest string) {
	if server.Tokens < numTokens {
		log.Fatalf("Server %v attempted to send %v tokens when it only has %v\n",
			server.Id, numTokens, server.Tokens)
	}
	message := TokenMessage{numTokens}
	server.sim.logger.RecordEvent(server, SentMessageEvent{server.Id, dest, message})
	// Update local state before sending the tokens
	server.Tokens -= numTokens
	link, ok := server.outboundLinks[dest]
	if !ok {
		log.Fatalf("Unknown dest ID %v from server %v\n", dest, server.Id)
	}
	link.events.Push(SendMessageEvent{
		server.Id,
		dest,
		message,
		server.sim.GetReceiveTime()})
}

// Callback for when a message is received on this server.
// When the snapshot algorithm completes on this server, this function
// should notify the simulator by calling `sim.NotifySnapshotComplete`.
func (server *Server) HandlePacket(src string, message interface{}) {
	// TODO: IMPLEMENT ME
	switch msg := message.(type) {
	case TokenMessage:
		// if len(server.snapShots.internalMap) > 0 {
			// In the middle of snap, recording message if
		server.snapShots.Range(func(k, v interface{}) bool {
			snap := v.(*ServerSnapShot)
			if _, ok := snap.peers[src]; !ok {
				snap.messages[src] = append(snap.messages[src], &SnapshotMessage{src, server.Id, msg})
			}

			return true
		})
			// for _,sid := range server.snapShots {
			// 	snap := server.snapShots[sid]
			// 	// peers did not send marker to me
			// 	if _, ok := snap.peers[src]; !ok {
			// 		snap.messages[src] = append(snap.messages[src], &SnapshotMessage{str, server.Id, msg})
			// 	}
			// }
		// }

		server.Tokens += msg.numTokens
	case MarkerMessage:
		snapShotId := msg.snapshotId

		// fmt.Println("MarkerMessage", snapShotId, server.Id, src)
		// si, ok := server.snapShots.Load(snapShotId)

		// if !ok {
		// 	server.StartSnapshot(snapShotId)
		// } 
		server.StartSnapshot(snapShotId)

		si, _ := server.snapShots.Load(snapShotId)

		snapshot := si.(*ServerSnapShot)
		snapshot.peers[src] = voidmember


        // fmt.Printf("%s started snapshot %d\n", len(snapshot.peers), snapshotId)
		if len(snapshot.peers) == len(server.inboundLinks) {
			server.sim.NotifySnapshotComplete(server.Id, snapShotId)
		}
	}
}

// Start the chandy-lamport snapshot algorithm on this server.
// This should be called only once per server.
func (server *Server) StartSnapshot(snapshotId int) {
	// TODO: IMPLEMENT ME
	_, ok := server.snapShots.Load(snapshotId)
	if ok{
		return
	}

	// fmt.Printf("%s started snapshot %d\n", server.Id, snapshotId)

	snapShot := NewServerSnapShot()
	snapShot.tokens = server.Tokens
	server.snapShots.Store(snapshotId, snapShot)

	markMsg := MarkerMessage{snapshotId}

	server.SendToNeighbors(markMsg)
}
