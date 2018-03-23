package study.algorithm.fms;

/**
 * 该状态机是确定性有限状态机，每一个输入之后的状态只能转换为其中一种状态。
 * 即：给定当前状态和输入，那么下一个状态是唯一确定的。DFA
 *    若给定当前状态和输入，下一个状态可能有多个则为NFA
 *    NFA可以转化为DFA
 *    正则表达式也都可以转换成NFA,而NFA转成DFA,最终可以实现正则表达式匹配的
 *    机制。
 *    在CEP复杂事件分析中也是多个简单事件状态的转换、组合而形成的复杂事件,意味
 *    着NFA是可以做为复杂事件分析的一种方式手段。
 * 
 *
 */
public class FiniteStateMachine
{

	private int currentState = 0;// 状态机的初始化状态为0

	private int preState = FMS.STATE_FAILURE;// 前一个状态

	private byte aheadChar = Input.EOF;// 将要读取的字符

	private int aheadInputState = FMS.STATE_FAILURE;// 预读取字符得到的下一个状态

	private Input input = new Input();

	private TableFMS fms = new TableFMS();

	private boolean endOfReads = false;

	public FiniteStateMachine()
	{
		input.ii_newFile(null); // 通过控制台输入
		input.ii_advance(); // 获取控制台输入
		input.ii_pushback(1);
		input.ii_mark_start();
	}

	public void compute()
	{
		while (true)
		{
			while (true)
			{
				if ((aheadChar = input.ii_lookahead(1)) != Input.EOF)
				{
					aheadInputState = fms.computeState(currentState, aheadChar);
					break;
				} else
				{
					endOfReads = true;
					if (preState != FMS.STATE_FAILURE)
					{
						currentState = FMS.STATE_FAILURE;
						break;
					} else
					{
						return;
					}
				}
			}

			if (aheadInputState != FMS.STATE_FAILURE)// 如果预读取的字符对应的状态不是无效或失败状态
			{
				System.out.println("Transition from state : " + currentState + "  to state : " + aheadInputState
						+ " on input char: " + (char) aheadChar);

				input.ii_advance();// 越过当前字符、准备读取下一个字符

				if (fms.isAcceptable(aheadInputState))
				{
					int tempState = currentState;
					preState = tempState;
					input.ii_mark_end();
				}
				currentState = aheadInputState;

			} else
			{
				if (preState == FMS.STATE_FAILURE)
				{
					if (aheadChar != '\n')
					{
						System.out.println("Ingoring bad input");
					}

					input.ii_advance(); // 忽略导致状态机出错的字符
				} else
				{
					input.ii_to_mark(); // 准备获取接收状态的字符形成的字符串
					System.out.println("Acceting state: " + preState);
					System.out.println("line: " + input.ii_lineno() + " accept text: " + input.ii_text());

					switch (preState)
					{				
					case 1:
						System.out.println(" it is a Integer");
						break;
					case 2:
					case 4:
						System.out.println(" it is a float point number");
						break;
					default:
						System.out.println(" internal error");
					}

				}

				// 识别到错误字符或给出判断后，将状态机重置
				preState = FMS.STATE_FAILURE;
				currentState = 0;
				input.ii_mark_start();

			}

			if (endOfReads)
			{
				return;
			}

		}

	}

	public static void main(String[] args)
	{
		FiniteStateMachine fms = new FiniteStateMachine();
		fms.compute();
	}

}
