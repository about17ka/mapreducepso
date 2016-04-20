package test;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class job1Reducer extends Reducer <DoubleWritable, Text, DoubleWritable,Text> 
{
	private DoubleWritable job1reduce_key = new DoubleWritable();
    private Text job1reduce_value = new Text();
    static int dim = 2;
    static int sizepop = 20;
    public double[] pop = new double[dim];	//粒子的位置
    public double[] dpbest = new double[dim];	//粒子记录的最优位置
    public double[] V = new double[dim];	//粒子的速度
    public double[] gbest = new double[dim];	//全局最优解的位置
    public double[] gbest_temp = new double[dim];	//临时全局最优解的位置
    public String[] S_value = new String[dim];
    public String F_value;
    public double m_dFitness =Double.MAX_VALUE;	//飞行后的粒子适应值
    //每一个key value对都要做对比，并不是各做各的
    public void reduce(DoubleWritable key,Iterable<Text> values,
            Context context) throws IOException, InterruptedException
    {
    	//在Reduce中也是一个一个<key,value>对去进行操作的
    	//key是double，value是Text
    	Double bestfitness = Double.valueOf(key.toString());
    	//先把key转换成粒子最优解
    	//map中key为最优解，value为F_value
        int k;
        if(bestfitness < m_dFitness) //若最优解优于当前的适应值
        {
            m_dFitness = bestfitness;	//先将当前适应值设置为最优解
            for (Text val : values)		//对于key相同的所有value
            {
               String itr[] = val.toString().split(" ");
               k = 0;
               for (int j = 0; j < dim; j++)
               {
                   pop[j] =Double.valueOf((itr[k++]));
                   V[j] =Double.valueOf((itr[k++]));
                   dpbest[j]= Double.valueOf((itr[k++]));
               }
               //先还原出具有相同最优解的粒子的位置，速度以及最优解所在位置
               for (int j = 0; j < dim; j++)
               {
                   gbest[j] =dpbest[j];
                  gbest_temp[j] = dpbest[j];
               }
               //将全局最优解位置设置为粒子所记录的最优解位置
               //同时将粒子所记录的最优解位置赋给gbest_temp
               F_value = "";
               for (int j = 0; j < dim; j++)
               {
                   S_value[j]= Double.toString(pop[j]) + " "
                         + Double.toString(V[j]) + " "
                         + Double.toString(dpbest[j]) + " "
                         + Double.toString(gbest[j]) + " ";
               }
               for (int j = 0; j < dim; j++)
               {
                   F_value +=S_value[j];
               }
               //还是将粒子的位置，速度，局部最优解和全局最优解记录到F_value中
               F_value += (Double.toString(bestfitness)) + " "
                      +(Double.toString(m_dFitness));
               //还要加上局部最优解和全局最优解的值
               job1reduce_key.set(1);
               job1reduce_value.set(F_value);
               context.write(job1reduce_key,job1reduce_value);
            }
         }
        else		//若最优解并没有当前值好
        {
            for (Text val : values)		////对于key相同的所有value
            {
               String itr[] = val.toString().split(" ");
               k = 0;
               for (int j = 0; j < dim; j++)
               {
                   pop[j] =Double.valueOf((itr[k++]));
                   V[j] =Double.valueOf((itr[k++]));
                   dpbest[j]= Double.valueOf((itr[k++]));
               }
               for (int j = 0; j < dim; j++)
               {
                   gbest[j] =gbest_temp[j];
               }
               //
               F_value = "";
               for (int j = 0; j < dim; j++)
               {
                   S_value[j]= Double.toString(pop[j]) + " "
                         + Double.toString(V[j]) + " "
                         + Double.toString(dpbest[j]) + " "
                         + Double.toString(gbest[j]) + " ";
               }
               for (int j = 0; j < dim; j++)
               {
                   F_value +=S_value[j];
               }
               F_value += (Double.toString(bestfitness)) + " "
                      +(Double.toString(m_dFitness));
               job1reduce_key.set(1);
               job1reduce_value.set(F_value);
               context.write(job1reduce_key,job1reduce_value);
              //还是将粒子的位置，速度，局部最优解和全局最优解记录到F_value中
            }
         }
    }
}
