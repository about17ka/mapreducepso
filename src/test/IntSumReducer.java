package test;
import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class IntSumReducer extends Mapper <Object, Text, DoubleWritable,Text> 
{
	private DoubleWritable job1map_key = new DoubleWritable();
	private Text job1map_value = new Text();
    static int dim = 2;	//优化函数的维数
    static int sizepop = 20;	//粒子数
    private final double w = 1;	//惯性系数
    static double c1 = 1;	//加速度
    static double c2 = 1;	//加速度
    public double[] pop = new double[dim];	//粒子的位置
    public double[] dpbest = new double[dim];	//粒子本身的最优位置
    public double[] V = new double[dim];	//粒子的速度
    public double[] fitness = new double[sizepop];
    public double[] gbest = new double[dim];
    public String[] S_value = new String[dim];	//global best fitness保留全局最优值的坐标
    public String F_value;
    public double bestfitness;
    public double m_dFitness;	//飞行后的粒子适应值
    private Random random = new Random();
    double g;
    double sum1;
    double sum2;
    public void map(Object key, Text values, Context context)
            throws IOException,InterruptedException
    {
    	//在mapreduce中，每个map任务处理的实际上是一行数据
    	String itr[] = values.toString().split("\\s");	//以空白字符分割
        int k =1;	//k赋1的原因是第0号元素为编号
        F_value ="";
        for (int j= 0; j < dim; j++) 
        {
           pop[j] =Double.valueOf((itr[k++]));
           V[j] =Double.valueOf((itr[k++]));
           dpbest[j] =Double.valueOf((itr[k++]));
           gbest[j] =Double.valueOf((itr[k++]));
        }
        bestfitness = Double.valueOf((itr[k++]));
        g = Double.valueOf((itr[k++]));
        //分别给出粒子的各数数数
        //粒子的位置*2 + 粒子的速度*2 + 粒子本身的最优解的位置*2 + 粒子记录全局最优解的位置*2 + 局部最优解 + 全局最优解 总共10个数
        for (int i= 0; i < dim; i++) 
        {
            V[i] = w * V[i] + c1 *random.nextDouble()
                   *(dpbest[i] - pop[i]) + c2 * random.nextDouble()
                   *(gbest[i] - pop[i]);
            pop[i] = pop[i] + V[i];
            //向着粒子本身最优解的位置飞行
         }
        //粒子飞行
        sum1 =0;
        sum2 =0;
        //计算Ackley 函数的值
        for (int i= 0; i < dim; i++) 
        {
           sum1 += pop[i] *pop[i];
           sum2 += Math.cos(2 * Math.PI* pop[i]);
        }
        //m_dFitness 计算出的当前值
        m_dFitness= -20 * Math.exp(-0.2 * Math.sqrt((1.0 / dim) * sum1))
              - Math.exp((1.0 / dim) * sum2) + 20 +2.72;
        if(m_dFitness < 0) 
        {
            System.out.println(sum1 + " "+ m_dFitness + " " + sum2);
        }
        //飞行后的粒子适应值与粒子本身记录的最优解进行比较
        //如果飞行后的粒子适应值优于粒子本身记录的最优解，则更新粒子本身记录的最优解及最优解位置
        if(m_dFitness < bestfitness) 
        {
            bestfitness = m_dFitness;
            for (int i = 0; i< dim; i++) 
            {
               dpbest[i] = pop[i];
            }
         }
        for (int j= 0; j < dim; j++) 
        {
            S_value[j] =Double.toString(pop[j]) + " "
                   +Double.toString(V[j]) + " "
                   +Double.toString(dpbest[j]) + " ";
        }
        //S_value中存放着粒子位置，速度以及最优解位置信息的一半
        for (int j= 0; j < dim; j++) 
        {
            F_value += S_value[j];
        }
       //F_value中存放着粒子位置，速度以及最优解位置信息的全部
        job1map_key.set(bestfitness);
        job1map_value.set(F_value);
        //System.out.println(bestfitness);
       //设置key为最优解，value为F_value，进入reduce
        context.write(job1map_key, job1map_value);
     }    
}
