package project2;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.*;

public class PageRankBlockReducer extends Reducer<Text, Text, Text, Text> {

	//private HashMap<String, Float> oldPR = new HashMap<String, Float>(800000);
	//private HashMap<String, Float> newPR = new HashMap<String, Float>(800000);
	//private HashMap<String, String> edgeList = new HashMap<String, String>(800000);
	//private HashMap<String, Integer> degrees = new HashMap<String, Integer>(800000);
	//private HashMap<String, ArrayList<String>> BE = new HashMap<String, ArrayList<String>>(800000);
	//private HashMap<String, ArrayList<String>> BC = new HashMap<String, ArrayList<String>>(800000);
	
	private project2.SoftLinkedCache<String, Float> oldPR = new project2.SoftLinkedCache<String, Float>(800000);
	private project2.SoftLinkedCache<String, Float> newPR = new project2.SoftLinkedCache<String, Float>(800000);
	private project2.SoftLinkedCache<String, String> edgeList = new project2.SoftLinkedCache<String, String>(800000);
	private project2.SoftLinkedCache<String, Integer> degrees = new project2.SoftLinkedCache<String, Integer>(800000);
	private HashMap<String, ArrayList<String>> BE = new HashMap<String, ArrayList<String>>(800000);
	private HashMap<String, ArrayList<String>> BC = new HashMap<String, ArrayList<String>>(800000);
	
	
	private ArrayList<String> vList = new ArrayList<String>();
	private Float dampingFactor = (float) 0.85;
	private Float randomJumpFactor = (1 - dampingFactor) / PageRankBlock.totalNodes;
	
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
		// format of each incoming value is either: <pageRankFactor>
		// this gives the pageRankFactor (pageRank/degrees) for each node pointing to the key node
		// or it could be: PR <prevPageRank> <outgoingEdgeList>
		Iterator<Text> itr = values.iterator();
		Text input = new Text();
		String[] inputTokens = null;
		
		Float pageRankIncomingSum = (float) 0.0;
		
		Float pageRankNew = (float) 0.0;
		Float pageRankOld = (float) 0.0;
		Float residualError = (float) 0.0;
		
		String output = "";
		
		ArrayList<String> temp = new ArrayList<String>();
		
		while (itr.hasNext()) {
			input = itr.next();
			inputTokens = input.toString().split("\\s+");			
			// if PR, it is the previous pagerank and outgoing edgelist for this node
			if (inputTokens[0].equals("PR")) {
				String nodeID = inputTokens[1];
				pageRankOld = Float.parseFloat(inputTokens[2]);
				oldPR.put(nodeID, pageRankOld);
				newPR.put(nodeID, 0.0f);
				if (inputTokens.length == 4) {
					edgeList.put(nodeID, inputTokens[3]);		
					degrees.put(nodeID, inputTokens[3].split(",").length);
				} else {
					edgeList.put(nodeID, "");
					degrees.put(nodeID, 0);
				}
				vList.add(nodeID);
				
			// if BE, it is a set of in-block edges
			} else if (inputTokens[0].equals("BE")) {
							
				if(BE.containsKey(inputTokens[2])){
					//Initialize BC for this v
					temp = BE.get(inputTokens[2]);
				}else{
					temp = new ArrayList<String>();
				}
				
				temp.add(inputTokens[1]);
				BE.put(inputTokens[2], temp);
				
			// if BC, it is an incoming node from outside of the block
			} else if (inputTokens[0].equals("BC")) {
				//System.out.println(BC.get(inputTokens[2]));
				
				if(BC.containsKey(inputTokens[2])){
					//Initialize BC for this v
					temp = BC.get(inputTokens[2]);
				}else{
					temp = new ArrayList<String>();
				}
				
				//System.out.println(temp.toString());
				//System.out.println(inputTokens[3]);
				String concatenateString = inputTokens[1] + "," + inputTokens[3];
				temp.add(concatenateString);
				BC.put(inputTokens[2], temp);
			
			}		
		}
		
		for(int i=0; i<1; i++) {
			IterateBlockOnce();
		}
				
		// compute the residual error for each node in this block
		for (String v : vList) {
			residualError += Math.abs(oldPR.get(v) - newPR.get(v)) / newPR.get(v);
		}
		residualError = residualError / vList.size();
		
		// add the residual error to the counter that is tracking the overall sum (must be expressed as a long value)
		long residualAsLong = (long) Math.floor(residualError * PageRankBlock.precision);
		context.getCounter(PageRankBlock.ProjectCounters.RESIDUAL_ERROR).increment(residualAsLong);
		
		// output should be 
		//	key:nodeID (for this node)
		//	value:<pageRankNew> <degrees> <comma-separated outgoing edgeList>
		for (String v : vList) {
			output = newPR.get(v) + " " + degrees.get(v) + " " + edgeList.get(v);
			Text outputText = new Text(output);
			Text outputKey = new Text(v);
			context.write(outputKey, outputText);
		}
		
		//System.out.print("The V List Is: ");
		//System.out.println(vList.size());
	}
	

	// v is all nodes within this block B
	// u is all nodes pointing to this set of v
	// some u are inside the block as well, those are in BE
	// some u are outside the block, those are in BC
	// PR[v] = current PageRank value of Node v
	// BE = the Edges from Nodes in Block B
    // BC = the Boundary Conditions
	// 		R = PR[u]/deg[u] for boundary nodes
	// NPR[v] = Next PageRank value of Node v
	protected void IterateBlockOnce() {
		//HashMap<String,Float> NPR = new HashMap<String,Float>(800000);
		project2.SoftLinkedCache<String,Float> NPR = new project2.SoftLinkedCache<String,Float>(800000);
		ArrayList<String> uList = new ArrayList<String>();
		ArrayList<String> uListBC = new ArrayList<String>();
		
		//for( v in B ) { NPR[v] = 0; }
		for (String v : vList) {
			NPR.put(v, 0.0f);
		}
		
	    //for( v in B ) {
		for (String v : vList) {
			
			
			if(BE.containsKey(v)){
				//Initialize BC for this v
				uList = BE.get(v);
			}else{
				uList = new ArrayList<String>();
			}
			
			//for( u where <u, v> in BE ) {
			for (String u : uList) {
				//    NPR[v] += PR[u] / deg(u);
				float npr = NPR.get(v) + (newPR.get(u) / degrees.get(u));
				NPR.put(v, npr);
	        }
			
			
			if(BC.containsKey(v)){
				//Initialize BC for this v
				uListBC = BC.get(v);
			}else{
				uListBC = new ArrayList<String>();
			}
			
			//for( u, R where <u,v,R> in BC ) {
			for (String uR : uListBC) { 
		        //    NPR[v] += R;
				String[] tempUR = uR.split(",");
				float npr = NPR.get(v) + Float.parseFloat(tempUR[1]);
				NPR.put(v, npr);
	        }
	        //NPR[v] = d*NPR[v] + (1-d)/N;
			float npr = (dampingFactor * NPR.get(v)) + randomJumpFactor;
			NPR.put(v, npr);
	    }
		
	    //for( v in B ) { PR[v] = NPR[v]; }
		for (String v : vList) {
			newPR.put(v, NPR.get(v));
		}
		System.gc();
	}
}

