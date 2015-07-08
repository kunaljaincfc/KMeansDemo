package com.datatorrent.demos.kmeans;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Context.PortContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.lib.ml.KMeansAggregator;
import com.datatorrent.lib.ml.KMeansCentroidUpdater;
import com.datatorrent.lib.ml.KMeansClusterer;
import com.datatorrent.lib.ml.KMeansConfig;
import com.datatorrent.lib.ml.KMeansInputReader;
import com.datatorrent.lib.ml.KMeansLineReader;
import com.datatorrent.lib.ml.KMeansOutputWriterOperator;
 
@ApplicationAnnotation(name="KMeansDemo")
public class Application implements StreamingApplication
{
   public void populateDAG(DAG dag, Configuration conf)
   {
	   int numClusters;
	   int maxIterations;
	   int dataDimension;
	   String inputDataFile;
	   String centroidListDir;
	   String centroidListFileName;
	   boolean fs;
	   long maxEmittedTuples;
	   
	   numClusters = Integer.parseInt(conf.get("dt.ml.kmeans.numClusters"));
	   maxIterations = Integer.parseInt(conf.get("dt.ml.kmeans.maxIterations"));
	   dataDimension=Integer.parseInt(conf.get("dt.ml.kmeans.dataDimension"));
	   inputDataFile = conf.get("dt.ml.kmeans.inputDataFile");
	   centroidListDir = conf.get("dt.ml.kmeans.centroidListDir");
	   centroidListFileName = conf.get("dt.ml.kmeans.centroidListFileName");
	   fs=conf.getBoolean("dt.ml.kmeans.fs", false);
	   maxEmittedTuples=Long.parseLong(conf.get("dt.ml.kmeans.maxEmittedTuples"));
	   
	   System.out.println(numClusters);
	   System.out.println(maxIterations);
	   System.out.println(dataDimension);
	   System.out.println(inputDataFile);
	   System.out.println(centroidListDir);
	   System.out.println(centroidListFileName);
	   System.out.println(fs);
	   System.out.println(maxEmittedTuples);
	   
	
	   
	   KMeansConfig kmc =new KMeansConfig(numClusters, maxIterations, dataDimension, inputDataFile, centroidListDir, centroidListFileName, fs, maxEmittedTuples);

	   KMeansLineReader linereader = dag.addOperator("LineReader", new KMeansLineReader(kmc));
	   KMeansInputReader inputreader = dag.addOperator("CSVParser", new KMeansInputReader(kmc));
	   KMeansClusterer kmeansclusterer = dag.addOperator("Clusterer", new KMeansClusterer(kmc));
	   KMeansAggregator kmeansaggregator = dag.addOperator("Aggregator", new KMeansAggregator(kmc));
	   KMeansCentroidUpdater kmeanscentroidupdater = dag.addOperator("CentroidUpdater", new KMeansCentroidUpdater(kmc));
	   KMeansOutputWriterOperator kmeansoutputwriteroperator = dag.addOperator("OutputWriter", new KMeansOutputWriterOperator(kmc));
    
    
    
	   dag.addStream("InputVectorLine", linereader.lineOutputPort, inputreader.input);
	   dag.addStream("CentroidListInput", linereader.centroidListOutputPort, inputreader.inputCentroidList);
	   dag.addStream("IterationCountSignal", linereader.iterationCountOutputPort, kmeanscentroidupdater.inputPortIterationCount);
    
	   dag.addStream("ParsedInputVector", inputreader.pointVectorOut, kmeansclusterer.inPoints);
	   dag.addStream("ParsedCentroidList", inputreader.CentroidListOut, kmeansclusterer.inCentroidList);
	   /*Thread Local Settings
	   dag.addStream("ParsedInputVector", inputreader.pointVectorOut, kmeansclusterer.inPoints).setLocality(Locality.THREAD_LOCAL);
	   dag.addStream("ParsedCentroidList", inputreader.CentroidListOut, kmeansclusterer.inCentroidList).setLocality(Locality.THREAD_LOCAL);
	
	   dag.setInputPortAttribute(kmeansclusterer.inPoints, PortContext.PARTITION_PARALLEL, true);
	   dag.setInputPortAttribute(kmeansclusterer.inCentroidList, PortContext.PARTITION_PARALLEL, true);
	   */
	   dag.addStream("ClusteredData", kmeansclusterer.clusterCenterOutput, kmeansaggregator.inpointcenter);
    
	   dag.addStream("AggregatedClusteredData", kmeansaggregator.centroidCountOutput, kmeanscentroidupdater.insumcount);
    
	   dag.addStream("UpdatedCentroidList", kmeanscentroidupdater.centroidUpdatedOutput,kmeansoutputwriteroperator.inCentroidListWriter);
	   
   }

}