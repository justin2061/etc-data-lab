package etc.dataprocess.test;

import org.apache.commons.lang.StringUtils;

public class LevenshteinTest {

	public static void main(String[] args) {
		String s1 = "本書係TED唯一官方版正式授權之成功演講術及說話技巧分享。不僅揭開TED如何起死回生的第一手幕後祕辛。也透過讓TED崛起之第一推手的現身說法，分享歷次成功演講的事前溝通內幕。一場成功且簡短的演講，不僅能夠激動人心，傳播知識，也能在倡導共同夢想的同時，徹底改變聽眾的世界觀，其影響力遠遠勝過任何書寫文字。";
		String s2 = "本書解釋動人的演講如何創造神奇的力量，並提供全方位指導，教你如何準備一場成功的演講。這沒有什麼成規處方，因為沒有、也不應該有一模一樣的任何兩場演講，但有一些工具可以幫助任何一位演講人。";
		
		int dis = StringUtils.getLevenshteinDistance(s1, s2);
		
		System.out.println("distance: "+dis);

	}

}
