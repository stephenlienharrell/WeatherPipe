
public class argTypeCheck {
	
	public static void main(String[] args){
		String a = "GGGG";
		System.out.println(checkType(a));
	}
	
	public static boolean checkType (String s) throws NullPointerException {
		try {
			if (s.isEmpty()){
				System.out.println("error: the string is empty");
				return false;
			}
			if (s.length() != 4) {
				System.out.println("error: the length of the string is not 4");
				return false;
			}
			for (int i = 0; i < s.length(); i++){
				if (s.charAt(i) > 90 || s.charAt(i) < 65){
					System.out.println("error: the string can only contain upper case letters");
					return false;
				}
			}
		}catch(NullPointerException e){
			System.out.println("error: the string is null");
			return false;
		}
		return true;
	}
}
