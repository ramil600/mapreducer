package mr

var (
	mapTask    = "map"
	reduceTask = "reduce"
	exitTask   = "exit"
)

type Task struct {
	WorkerNum int
	Name      string // mapf or reducef
	FileName  string //name of the file to map or suffix of the file to reduce
	Buckets   int
}
