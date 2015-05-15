package em;

import Jama.*;

//4D gaussian specific to OPP
public class Gaussian {
	double[] mean;
	Matrix sigma;
	Matrix sigmaInv;
	double pi;
	double det;
	int dimensions;
	
	public Gaussian(String s) {
		String[] tmp = s.split(",");
		dimensions = Integer.parseInt(tmp[0]);
		
		mean = new double[dimensions];
		double[][] sig = new double[dimensions][dimensions];
		
		for (int i = 0; i < dimensions; i++) {
			mean[i] = Double.parseDouble(tmp[i+1]);
		}
		
		for (int i = 0; i < dimensions; i++) {
			for (int j = 0; j < dimensions; j++) {
				sig[i][j] = Double.parseDouble(tmp[1 + (i+1)*dimensions + j]);
			}
		}
		
		sigma = new Matrix(sig, dimensions, dimensions);
		det = sigma.det();
		sigmaInv = sigma.inverse();
		pi = Double.parseDouble(tmp[tmp.length-1]);
	}
	
	public Gaussian(double[] m, double[][] s, double p, int dimensions) {
		mean = m;
		sigma = new Matrix(s, dimensions, dimensions);
		System.out.print("VAR: ");
		for (int i = 0; i < dimensions; i++) {
			for (int j = 0; j < dimensions; j++) {
				System.out.print(sigma.get(i,j) + " ");
			}
		}
		System.out.println();
		pi = p;
		det = sigma.det();
		sigmaInv = sigma.inverse();
		this.dimensions = dimensions;
	}
	
	//add log pdf to deal with underflow
	
	public double log_pdf(double[] measurements) {
	    double[] diff = new double[dimensions];
        for (int i = 0; i < dimensions; i++) {
            diff[i] = measurements[i] - mean[i];
        }
        
        Matrix dif = new Matrix(diff, 1);
        double exp = dif.times(sigmaInv).times(dif.transpose()).get(0, 0);
        
        return -0.5 * (4 * Math.log(2*Math.PI) + Math.log(det) + exp);
	}
	
	public double pdf(double[] measurements) {
		double denominator = 1.0 / Math.sqrt(Math.pow(2*Math.PI, 4) * det);
		
		double[] diff = new double[dimensions];
		for (int i = 0; i < dimensions; i++) {
			diff[i] = measurements[i] - mean[i];
		}
		
		
		Matrix dif = new Matrix(diff, 1);
		double exp = -0.5 * dif.times(sigmaInv).times(dif.transpose()).get(0, 0);
		
		return Math.exp(exp) * denominator;
	}
	
	public double[][] partialSigma(double[] measurements, double r) {
		double[] diff = new double[dimensions];
		for (int i = 0; i < dimensions; i++) {
			diff[i] = measurements[i] - mean[i];
		}
		
		Matrix dif = new Matrix(diff, 1);
		Matrix sig = dif.transpose().times(dif).times(r);
		
		return sig.getArray();
	}
	
	public double getPi() {
		return pi;
	}
	
	public int getDimensions() {
		return dimensions;
	}
	
	public String toString() {
		String s = dimensions + ",";
		
		for (int i = 0; i < dimensions; i++) {
			s += mean[i]; s += ",";
		}
		
		for (int i = 0; i < dimensions; i++) {
			for (int j = 0; j < dimensions; j++) {
				s += sigma.get(i, j); s += ",";
			}
		}
		
		s += pi;
		
		return s;
	}
	
	public static void main(String[] args) {
		int d = 4;
	    double[] mean = {26528,12637,1824,28493};
	    double[][] sigma = new double[d][d];
	    sigma[0][0] = 1; sigma[0][1] = 0; sigma[0][2] = 0; sigma[0][3] = 0;
	    sigma[1][0] = 0; sigma[1][1] = 1; sigma[1][2] = 0; sigma[1][3] = 0;
	    sigma[2][0] = 0; sigma[2][1] = 0; sigma[2][2] = 1; sigma[2][3] = 0;
	    sigma[3][0] = 0; sigma[3][1] = 0; sigma[3][2] = 0; sigma[3][3] = 1;
	    
	    double pi = 1.0 / 7;
	    
	    Gaussian g = new Gaussian(mean, sigma, pi, d);
	    double[] measurements = {30755,11771,25587,17613};
	    System.out.println(g.pdf(measurements));
	    System.out.println(g.log_pdf(measurements));
	}
}
