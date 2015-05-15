package em;

import java.util.ArrayList;

public class EMparams {
	private int k;
	private Gaussian[] gaussians;
	
	
	public EMparams (ArrayList<String> lines) {
		k = lines.size();
		gaussians = new Gaussian[k];
		for (int i = 0; i < k; i++) {
			gaussians[i] = new Gaussian(lines.get(i));
		}
	}

	public int getK() {
		return k;
	}

	public Gaussian[] getGaussians() {
		return gaussians;
	}
}
