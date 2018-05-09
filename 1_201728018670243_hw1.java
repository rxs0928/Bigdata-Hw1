/**
 * @author Ren Xueshuang
 *
 */


import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;

import org.apache.log4j.*;



public class Hw1Grp1 {

	/**
	 * @param args include four parts,e.g R=/hw1/file.tbl S=/hw1/file.tbl join:R0=S0 res:S1,R3 *
	
	 */
	public static void main(String[] args) throws IOException, URISyntaxException {
		
		
		/*Read the command line arguments*/
		String file1=args[0].split("=")[1];
	 	String file2=args[1].split("=")[1];//file1 and file2 denote two file pathes
	 	String file1_ID=args[0].split("=")[0];
	 	String file2_ID=args[1].split("=")[0];//file1_ID denotes the input name of file ,e.g 'R'、'S' or other namae
	 	
	 	String str[]=args[2].substring(5).split("=");		 		 	    	
    		int file1_join_colum=0;
	 	int file2_join_colum=0;	 	
	 	for(int i=0;i<2;i++){
	 		if(str[i].matches(file1_ID+"\\d+")){
	 			file1_join_colum=Integer.parseInt(str[i].substring(file1_ID.length()));
	 		 	} 
	 		else if (str[i].matches(file2_ID+"\\d+")){
	 			file2_join_colum=Integer.parseInt(str[i].substring(file2_ID.length()));
	 			}
	 		else
	 			{System.out.println("Please check your input about join_columns");}
	 		}
	 	

	 	String str1[]=args[3].substring(4).split(",");		 	
	 	ArrayList<Integer> file1_res_colum=new ArrayList<Integer>();
    		ArrayList<Integer> file2_res_colum=new ArrayList<Integer>();
	 	for(int i=0;i<str1.length;i++){
	 		if(str1[i].matches(file1_ID+"\\d+")){
	 			file1_res_colum.add(Integer.parseInt(str1[i].substring(file1_ID.length())));
	 			}
	 		else if (str1[i].matches(file2_ID+"\\d+")){
	 			file2_res_colum.add(Integer.parseInt(str1[i].substring(file2_ID.length())));
	 			}
	 		else
	 			{System.out.println("Please check your input about res_columns");}
	 		}
	
	 	
	/*<p>1.read Tbl file*/	 	
	List<List<String>> file1_tbl = HDFSread(file1);
    	List<List<String>> file2_tbl = HDFSread(file2);
    	
    	/*<p>2.sorting*/
    	sort(file1_tbl,file1_join_colum);
    	sort(file2_tbl,file2_join_colum);
    	
    	/*<p>3.Merging*/
    	
    	creatResult_tbl();
    	String tblname="Result";
    	Configuration configuration = HBaseConfiguration.create();
    	HTable table=new HTable(configuration,tblname);
    	
    	int p=file1_join_colum;
    	int q=file2_join_colum;
    	int index1=0;
    	int index2=0;
    	
	 	while(index1<file1_tbl.size() && index2<file2_tbl.size()){
	 		int count=0;
	 		if(file1_tbl.get(index1).get(p).compareTo(file2_tbl.get(index2).get(q))>0)
	 		{	index2++;	 }  
	 		else if (file1_tbl.get(index1).get(p).compareTo(file2_tbl.get(index2).get(q))<0)
	 		{	index1++;	 }  
	 		else
	 		{ 
	 			 			
			  	 //put file1_tbl(index1) into Result
	 			putResult(file1_ID,file1_tbl.get(index1),p,file1_res_colum,count);
	 			
	 			//put file2_tbl(index2) into Result
	 			putResult(file2_ID,file2_tbl.get(index2),q,file2_res_colum,count);
	 			 			
	 			index2++;
	 			while(index2<file2_tbl.size()&&file1_tbl.get(index1).get(p).compareTo(file2_tbl.get(index2).get(q))==0){
	 				count++;
		 			putResult(file1_ID,file1_tbl.get(index1),file1_join_colum,file1_res_colum,count);
		 			putResult(file2_ID,file2_tbl.get(index2),file2_join_colum,file2_res_colum,count);
		 			index2++;
	 			}
	 			
	 			index1++;
	 			int count_tmp=count;
	 			
	 			while(index1<file1_tbl.size()&&file1_tbl.get(index1).get(p).compareTo(file2_tbl.get(index2-1).get(q))==0){
	 				
	 				for(int i=count_tmp;i>=0;i--){
	 					count++;
	 					putResult(file1_ID,file1_tbl.get(index1),file1_join_colum,file1_res_colum,count);
			 			putResult(file2_ID,file2_tbl.get(index2-i-1),file2_join_colum,file2_res_colum,count);
	 					}
	 				index1++;
	 			}	 			
		 		
			//index2++;
	 		}
	
	 	}

	 	System.out.println("put successfully");
	 }
	 			
	 	  

	
	
	/*  @#see putResult <br>
         <p> @param    file_ID:the input name of file e.g R or S,  row:the satisfactory row ,rowkey:the join_key of file, res_column:the res_columns of file,count:the count of the rows withing same rowkey
*/
	 public static void putResult(String file_ID,List<String> row,int rowkey,ArrayList<Integer> res_column, int count) throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
	    	String tblname="Result";
	    	Configuration configuration = HBaseConfiguration.create();
	    	HTable table=new HTable(configuration,tblname);
	    	
	    	Put put = new Put(row.get(rowkey).getBytes());	 			
 			for (int i=0;i<res_column.size();i++){
 				int d=res_column.get(i);//d is an integer
 				String col_name;
 				if (count==0){
 					col_name=file_ID+d;
 				}
 				else{
 					col_name=file_ID+d+"."+count;
 				}	 				
 				put.add("res".getBytes(),col_name.getBytes(),row.get(d).getBytes());
 			}		
				table.put(put);
				table.close();
	 
	 }
	 
	 /* @#see HDFSread <br>
	@param file_path
	*/
	 public static List<List<String>> HDFSread(String file_path) throws IOException, URISyntaxException{
	    	String file= "hdfs://localhost:9000/" + file_path; 	

	        Configuration conf = new Configuration();
	        FileSystem fs = FileSystem.get(URI.create(file), conf);
	        Path path = new Path(file);
	        FSDataInputStream in_stream = fs.open(path);
	        BufferedReader in = new BufferedReader(new InputStreamReader(in_stream));
	        String s;
	        
	        List<String> row = new ArrayList<String>();
	    	List<List<String>> table = new ArrayList<List<String>>(); 
	        while ((s=in.readLine())!=null) {
	        	row = Arrays.asList(s.split("\\|"));
	        	table.add(row);
	        }

	        in.close();

	        fs.close();
	        
	        return table;
	    }
	 
	 
	 /* @#see sort <br>
 		@param LLs:file_convered_List<List<String>>,d:the join key of file*/
		public static void sort(List<List<String>> LLs,int d){
			for (int i=1;i<LLs.size();i++){
				for (int j=0;j<LLs.size()-1;j++){
					if (LLs.get(j).get(d).compareTo(LLs.get(j+1).get(d))>0){
						List<String> List_temp=LLs.get(j);
						LLs.set(j, LLs.get(j+1));
						LLs.set(j+1,List_temp);
					}
				}
			}
	    

		}
		
		/*@#see Creat Result_tbl <br> */
		public static void creatResult_tbl() throws MasterNotRunningException, ZooKeeperConnectionException, IOException {

		    Logger.getRootLogger().setLevel(Level.WARN);

		    // create table descriptor
		    String tableName= "Result";
		    HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(tableName));

		    // create column descriptor
		    HColumnDescriptor cf = new HColumnDescriptor("res");
		    htd.addFamily(cf);

		    // configure HBase
		    Configuration configuration = HBaseConfiguration.create();
		    HBaseAdmin hAdmin = new HBaseAdmin(configuration);

		    //if 'Result' table already exists,delete it
		    if (hAdmin.tableExists(tableName)) {
		    	hAdmin.disableTable(tableName);
		        hAdmin.deleteTable(tableName);
		        System.out.println("Existing 'Result' table has been deleted");	  	
		    }
		    //creat new 'Result' table
		        hAdmin.createTable(htd);
		        System.out.println("table "+tableName+ " created successfully");
		  	    hAdmin.close();
		  }
		 }

	


