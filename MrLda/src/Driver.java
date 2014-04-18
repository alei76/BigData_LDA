import Jama.*;
import Jama.util.Maths;

public class Driver {
	public int D;
	public int K;
	public int V;
	public double[] oldAlpha;
	public double[] newAlpha;
	public double[][] hessian;
	public double[] gradient;
	
//Use Jama package to invert matrix.
	
	public Driver(int Dsize, int Ksize, int Vsize){
		this.D = Dsize;
		this.K = Ksize;
		this.V = Vsize;
		
		this.oldAlpha = new double[this.K];
		this.newAlpha = new double[this.K];
		this.hessian = new double[this.K][this.K];
		this.gradient = new double[this.K];
	}
	
	public void retrieveAlpha(){
		/**
		 * TODO
		 */
	}
	
	public double[] retrieveReducerOutput(){
		return null;
		/**
		 * TODO
		 */
	}
	public double sumAlpha(){
		double sum = 0.0;
		for(int i = 0; i < K; i++){
			sum += oldAlpha[i];
		}
		return sum;
	}
	
	public void setHessian(){

		double nonDiag = D*MathFunctions.trigamma(sumAlpha());
		
		for(int i = 0; i < K; i++){
			for(int j = 0; j < K; j++){
				if(i==j){
					hessian[i][j] = D*MathFunctions.trigamma(oldAlpha[i]) -nonDiag;
				}
				else{
					hessian[i][j] = nonDiag;
				}
			}
		}
		
	}
	
	public void setGradient(){

		
		double[] rDelta = retrieveReducerOutput();
		double alphaSummed = MathFunctions.diGamma(sumAlpha());
		for(int i = 0; i<K; i++){
			gradient[i] = D*(alphaSummed - MathFunctions.diGamma(oldAlpha[i])) + rDelta[i];
		}
	}
	
	public void setNewAlpha(){

		retrieveAlpha();
		setGradient();
		setHessian();
		
		Matrix H = new Matrix(hessian);
		Matrix invH = H.inverse();
		
		double[][] invHessian = invH.getArray();
		
		this.newAlpha = MathFunctions.NetwtonRaphson(this.oldAlpha, invHessian, this.gradient);
		
		
		
		
		
	}
	
}