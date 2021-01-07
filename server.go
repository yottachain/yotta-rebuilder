package ytrebuilder

import (
	"context"

	pb "github.com/yottachain/yotta-rebuilder/pbrebuilder"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

// Server implemented server API for Analysis service.
type Server struct {
	Rebuilder *Rebuilder
}

//GetRebuildTasks implemented GetRebuildTasks function of Rebuilder
func (server *Server) GetRebuildTasks(ctx context.Context, req *pb.Int32Msg) (*pb.MultiTaskDescription, error) {
	result, err := server.Rebuilder.GetRebuildTasks(context.Background(), req.GetValue())
	if err != nil {
		return nil, status.Errorf(codes.Internal, err.Error())
	}
	return result, nil
}

//UpdateTaskStatus implemented UpdateTaskStatus function of Rebuilder
func (server *Server) UpdateTaskStatus(ctx context.Context, req *pb.MultiTaskOpResult) (*pb.Empty, error) {
	err := server.Rebuilder.UpdateTaskStatus(context.Background(), req)
	return &pb.Empty{}, err
}
