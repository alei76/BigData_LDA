
public class MathFunctions {
	
	/**
	 * Approximation of the digamma function
	 * @param x
	 * @return
	 */
	public static double diGamma(double x ){
		 double r = 0.0;

		    while (x <= 5) {
		      r -= 1 / x;
		      x += 1;
		    }

		    double f = 1.0 / (x * x);

		    double t = f
		        * (-0.0833333333333333333333333333333 + f
		            * (0.00833333333333333333333333333333 + f
		                * (-0.00396825396825396825 + f
		                    * (0.0041666666666666666666666667 + f
		                        * (-0.00757575757575757575757575757576 + f
		                            * (0.0210927960928 + f
		                                * (-0.0833333333333333333333333333333 + f * 0.44325980392157)))))));
		    return r + Math.log(x) - 0.5 / x + t;
	}
	
	
	 public static double trigamma(double x) {
		    double p;
		    int i;

		    x = x + 6;
		    p = 1 / (x * x);
		    p = (((((0.075757575757576 * p - 0.033333333333333) * p + 0.0238095238095238) * p - 0.033333333333333)
		        * p + 0.166666666666667)
		        * p + 1)
		        / x + 0.5 * p;
		    for (i = 0; i < 6; i++) {
		      x = x - 1;
		      p = 1 / (x * x) + p;
		    }


		    return p;
		  }
	
	public static double[] NetwtonRaphson(double[] alphaOld, double[][] hessian, double[] gradient ){
		int K = alphaOld.length;
		double[] result = new double[alphaOld.length];
		for(int i = 0; i < K; i++){
			result[i] = alphaOld[i];
		}
		for(int i = 0; i < K; i++){
			for(int j = 0; j < K; j++){
				result[i] += -(hessian[i][j]*gradient[j]);
			}
		}
		
		return result;
		
	}
	

}
