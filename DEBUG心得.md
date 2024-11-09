### Project2A
- 成为leader提交的空操作，不是entry为空，是entry大小为1，包含一个空{}
- Index不是真实数组中的Index
- RaftLog.Term(i)，i可能out of bound
- Vote时，Follower发现Candidate任期比自己大，会进行becomeFollower的操作，但是仅用来更新自己的term以及清空timeout，不一定投票。投票要看log的新旧程度的


### Project2B
- 需要完成一部分Snapshot，坑
- 将整个entry作为log写进RaftDB，换言之，RaftDB是做记录用的，不是存储真正的数据
- 尽管PeerStorage里有RaftLocalState，但是要求要把RaftLocalState“像log一样”写进RaftDB
- 上层发来的Requests可能是一批Request，在apply时要注意

### Project2C
- 快照生成是快照生成，由Raft内部Leader决定生成并发送给Follower
- 日志压缩是日志压缩，由上层Handle决定的，是AdminRequest，在Entry Apply时，通过processAdminRequest处理。日志压缩并不生成快照，它只是将超过数量阈值，并已经写入DB的Entry直接删除


### Project3B
- 有关Region的操作都要记得检查是否为本区域以及版本号
- Timeout问题，只剩两个节点，然后被移除的那个节点正好是 Leader。因为网络是 unreliable，Leader 广播给另一个 Node 的心跳正好被丢了，也就是另一个节点的 commit 并不会被推进，也就是对方节点并不会执行 remove node 操作。而这一切 Leader 并不知道，它自己调用 d.destroyPeer() 已经销毁了。此时另一个节点并没有移除 Leader，它会发起选举，但是永远赢不了，因为需要收到被移除 Leader 的投票。

以上问题仍然存在，发现因为Unreliable网络导致备选leader节点完成同步的消息可能被错过，而leader也不会再发新消息了，因此没有机会知道已经被同步完成了
但是解决了这个，仍然存在，比如leader总是在Snapshot，但是备选leader并不需要这个snapshot，而是需要正常的entry，但是并没有发回最新的next

解决这个，又有新的，依然是Unreliable的锅，split，主节点已经揽下了工作，并进行了分裂，但从节点分裂的msg丢失了。只有两个空entry的节点硬选leader,它以为有5个人，在等3票，实际上只有2人，TODO



- Unreliable中，scan,需要先获得key对应的regionID（无需Raft），然后通过regionID获得iter。“然后”之前，可能出现split的apply，此时key就不在原来region里了
在applyCommitedEntry时操作错误，之前是将多个entry都处理完了之后一起提交，但是现在这个就成了罪魁祸首



want but get，region1一开始是[0,无穷],want[4000,4001],一开始在region1扫描的好好的，半路分裂了

还有一种情况，也是分裂造成的，已经扫描完了所有的KV，但是准备给key（用以结束循环）赋值时，赋值为了分裂后的Endkey，此时旧不符合结束条件了


大多数错误已经修正，只有极个别情况：
