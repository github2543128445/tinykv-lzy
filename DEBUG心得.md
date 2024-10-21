### Project2A
- 成为leader提交的空操作，不是entry为空，是entry大小为1，包含一个空{}
- Index不是真实数组中的Index
- RaftLog.Term(i)，i可能out of bound
- Vote时，Follower发现Candidate任期比自己大，会进行becomeFollower的操作，但是仅用来更新自己的term以及清空timeout，不一定投票。投票要看log的新旧程度的