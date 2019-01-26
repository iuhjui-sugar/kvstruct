package hashmap

type Reply struct {
	err   error
	msg   string
	items [][]byte
}

func NewReply() *Reply {
	rep := new(Reply)
	rep.items = make([][]byte, 0, 1)
	return rep
}

func (rep *Reply) SetErr(err error) *Reply {
	rep.err = err
	return rep
}

func (rep *Reply) SetMsg(msg string) *Reply {
	rep.msg = msg
	return rep
}

func (rep *Reply) Push(item []byte) *Reply {
	rep.items = append(rep.items, item)
	return rep
}

func (rep *Reply) Error() error {
	return rep.err
}
