package study.algorithm.fms;

public interface FMS
{
	public static final int STATE_FAILURE=-1;
	
	//给定当前状态和当前输入、从而获取下一个状态
	public int computeState(int state,byte input);
	
	//给定的状态是否接受状态
	public boolean isAcceptable(int state);

}
