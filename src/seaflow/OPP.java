package seaflow;


public class OPP {

	private double[] measurements;
	private String file;
	private int lineNum;
	private int dimensions;
	
	public OPP(String s) {
		try {
			String[] tmp = s.split(",");
			file = tmp[0];
			lineNum = Integer.parseInt(tmp[1]);
			dimensions = Integer.parseInt(tmp[2]);
			measurements = new double[dimensions];
			for (int i = 0; i < dimensions; i++) {
				measurements[i] = Double.parseDouble(tmp[i+3]);
			}
		
		} catch (Exception e) {
			System.out.println("Exception caught in OPP Constructor\nProblem line :" + s);
		}
	}
	
	public OPP(String f, int ln, double[] m) {
		file = f;
		measurements = m;
		lineNum = ln;
		dimensions = m.length;
	}

	public double[] getMeasurements() {
		return measurements;
	}

	public String getFile() {
		return file;
	}

	public int getLineNum() {
		return lineNum;
	}
	
	public int getDimensions() {
	    return dimensions;
	}

}
