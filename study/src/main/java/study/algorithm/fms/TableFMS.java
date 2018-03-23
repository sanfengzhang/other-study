package study.algorithm.fms;

/**
 * 实现一个根据输入来判断：输入的数据类型是整型还是浮点型的自动机
 * 
 * @author hanl
 *
 */
public class TableFMS implements FMS
{

	//ASCLL码有128个，其中在该自动机中使用的有0-9和'.'和'e'
	private final int ASCLL_COUNT = 128;

	//总共存在的几种状态
	private final int STATE_COUNT = 6;

	//构建一个二维数组，行数是对应的自动机的状态数且第n行对应的是第n中状态
	private final int[][] fmsTable = new int[STATE_COUNT][ASCLL_COUNT];

	
	private final boolean[] accepte = new boolean[] { false, true, true, false, true, false };

	public TableFMS()
	{

		for (int i = 0; i < STATE_COUNT; i++)
		{
			for (int j = 0; j < ASCLL_COUNT; j++)
			{
				fmsTable[i][j] = -1;
			}

		}

		initPutForNumber(0, 1);
		initPutForNumber(1, 1);
		initPutForNumber(2, 2);
		initPutForNumber(3, 2);
		initPutForNumber(4, 4);
		initPutForNumber(5, 5);

		fmsTable[0]['.'] = 3;
		fmsTable[1]['.'] = 2;
		fmsTable[1]['e'] = 5;
		fmsTable[2]['e'] = 5;

	}

	private void initPutForNumber(int row, int val)
	{
		for (int i = 0; i < 10; i++)
		{
			fmsTable[row]['0' + i] = val;
		}
	}

	public int computeState(int state, byte input)
	{
		if (state == FMS.STATE_FAILURE || input >= ASCLL_COUNT)
		{
			return FMS.STATE_FAILURE;
		}

		return fmsTable[state][input];
	}

	public boolean isAcceptable(int state)
	{
		if (state != FMS.STATE_FAILURE)
		{
			return accepte[state];
		}

		return false;
	}

}
