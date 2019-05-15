package driver

type Driver interface {
	SetHeartBeat(node string)
	GetNodeList() []string
}

