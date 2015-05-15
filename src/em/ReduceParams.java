package em;

public class ReduceParams {
	double r;
	double[] measurements;
	int n; //sum of all n = N
	double[][] sigma;
	int dimensions;
	
	public ReduceParams(double _r, int _n, double[] m, double[][] s) {
		r = _r;
		measurements = m;
		n = _n;
		sigma = s;
		dimensions = m.length;
	}
	
	public ReduceParams(String s) {
		String[] tmp = s.split(",");
		dimensions = Integer.parseInt(tmp[0]);
		
		measurements = new double[dimensions];
		sigma = new double[dimensions][dimensions];
		
		r = Double.parseDouble(tmp[1]);
		n = Integer.parseInt(tmp[2]);
		for (int i = 0; i < dimensions; i++) {
			measurements[i] = Double.parseDouble(tmp[i + 3]);
		}
		
		for (int i = 0; i < dimensions; i++) {
			for (int j = 0; j < dimensions; j++) {
				sigma[i][j] = Double.parseDouble(tmp[3 + ((i+1) * dimensions) + j]);
			}
		}
	}
	
	public String toString() {
		String s = dimensions + ",";
		
		s += r;
		s += ",";
		s += n;
		s+= ",";
		
		for (int i = 0; i < dimensions; i++) {
			s += measurements[i]; s += ",";
		}
		
		for (int i = 0; i < dimensions; i++) {
			for (int j = 0; j < dimensions; j++) {
				s += sigma[i][j];
				if (!(i == dimensions-1 && j == dimensions-1)) {
					s += ",";
				}
			}
		}
		
		return s;
	}

	public double getR() {
		return r;
	}

	public double[] getMeasurements() {
		return measurements;
	}

	public int getN() {
		return n;
	}

	public double[][] getSigma() {
		return sigma;
	}

    public void incrementR(double r) {
        this.r += r;
    }

    public int getDimensions() {
    	return dimensions;
    }
    
    public void incrementMeasurements(double[] measurements) {
        for (int i = 0; i < dimensions; i++) {
            this.measurements[i] += measurements[i];
        }
    }

    public void incrementN(int n) {
        this.n += n;
    }

    public void incrementSigma(double[][] sigma) {
        for (int i = 0; i < dimensions; i++) {
            for (int j = 0; j < dimensions; j++) {
                this.sigma[i][j] += sigma[i][j];
            }
        }
    }
}
