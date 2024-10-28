### Project2A
- 成为leader提交的空操作，不是entry为空，是entry大小为1，包含一个空{}
- Index不是真实数组中的Index
- RaftLog.Term(i)，i可能out of bound
- Vote时，Follower发现Candidate任期比自己大，会进行becomeFollower的操作，但是仅用来更新自己的term以及清空timeout，不一定投票。投票要看log的新旧程度的


### Project2B
- 需要完成一部分Snapshot，坑
- 将整个entry作为log写进RaftDB，换言之，RaftDB是做记录用的，不是存储真正的数据
- 尽管PeerStorage里有RaftLocalState，但是要求要把RaftLocalState“像log一样”写进RaftDB


### Project2C
- 快照生成是快照生成，由Raft内部Leader决定生成并发送给Follower
- 日志压缩是日志压缩，由上层Handle决定的，是AdminRequest，在Entry Apply时，通过processAdminRequest处理。日志压缩并不生成快照，它只是将超过数量阈值，并已经写入DB的Entry直接删除


### 