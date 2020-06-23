package ytrebuilder

import (
	"context"

	pb "github.com/yottachain/yotta-rebuilder/pb"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

// Server implemented server API for Analysis service.
type Server struct {
	Rebuilder *Rebuilder
}

//GetRebuildTasks implemented GetRebuildTasks function of Rebuilder
func (server *Server) GetRebuildTasks(ctx context.Context, req *pb.Empty) (*pb.MultiTaskDescription, error) {
	result, err := server.Rebuilder.GetRebuildTasks()
	if err != nil {
		return nil, status.Errorf(codes.Internal, err.Error())
	}
	return result, nil
}

//UpdateTaskStatus implemented UpdateTaskStatus function of Rebuilder
func (server *Server) UpdateTaskStatus(ctx context.Context, req *pb.MultiTaskOpResult) (*pb.Empty, error) {
	server.Rebuilder.UpdateTaskStatus(req)
	return &pb.Empty{}, nil
}
