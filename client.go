package ytrebuilder

import (
	context "context"

	pb "github.com/yottachain/yotta-rebuilder/pb"
	"google.golang.org/grpc"
)

//RebuilderClient client of grpc call
type RebuilderClient struct {
	client pb.RebuilderClient
}

//NewClient create a new grpc client
func NewClient(addr string) (*RebuilderClient, error) {
	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	return &RebuilderClient{client: pb.NewRebuilderClient(conn)}, nil
}

//GetRebuildTasks client call of GetRebuildTasks
func (cli *RebuilderClient) GetRebuildTasks(ctx context.Context) (*pb.MultiTaskDescription, error) {
	result, err := cli.client.GetRebuildTasks(ctx, &pb.Empty{})
	if err != nil {
		return nil, err
	}
	return result, nil
}

//UpdateTaskStatus if node is selected for spotchecking
func (cli *RebuilderClient) UpdateTaskStatus(ctx context.Context, res *pb.MultiTaskOpResult) error {
	_, err := cli.client.UpdateTaskStatus(ctx, res)
	if err != nil {
		return err
	}
	return nil
}
