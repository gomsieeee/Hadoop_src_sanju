/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.mapred;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.server.jobtracker.TaskTracker;

/**
 * A {@link TaskScheduler} that keeps jobs in a queue in priority order (FIFO
 * by default).
 */
class JobQueueTaskScheduler extends TaskScheduler {
  
  private static final int MIN_CLUSTER_SIZE_FOR_PADDING = 3;
  public static final Log LOG = LogFactory.getLog(JobQueueTaskScheduler.class);
  
  protected JobQueueJobInProgressListener jobQueueJobInProgressListener;
  protected EagerTaskInitializationListener eagerTaskInitializationListener;
  private float padFraction;
  
  public JobQueueTaskScheduler() {
    this.jobQueueJobInProgressListener = new JobQueueJobInProgressListener();
  }
  
  @Override
  public synchronized void start() throws IOException {

  	LOG.info("Tigers: JobQueueTaskScheduler start()");
    super.start();
    taskTrackerManager.addJobInProgressListener(jobQueueJobInProgressListener);
    eagerTaskInitializationListener.setTaskTrackerManager(taskTrackerManager);
    eagerTaskInitializationListener.start();
    taskTrackerManager.addJobInProgressListener(
        eagerTaskInitializationListener);
  }
  
  @Override
  public synchronized void terminate() throws IOException {
    	LOG.info("Tigers: JobQueueTaskScheduler terminate()");
    if (jobQueueJobInProgressListener != null) {
      taskTrackerManager.removeJobInProgressListener(
          jobQueueJobInProgressListener);
    }
    if (eagerTaskInitializationListener != null) {
      taskTrackerManager.removeJobInProgressListener(
          eagerTaskInitializationListener);
      eagerTaskInitializationListener.terminate();
    }
    super.terminate();
  }
  
  @Override
  public synchronized void setConf(Configuration conf) {
    super.setConf(conf);
    padFraction = conf.getFloat("mapred.jobtracker.taskalloc.capacitypad", 
                                 0.01f);
    this.eagerTaskInitializationListener =
      new EagerTaskInitializationListener(conf);
  }

  @Override
  public synchronized List<Task> assignTasks(TaskTracker taskTracker)
      throws IOException {
		
    TaskTrackerStatus taskTrackerStatus = taskTracker.getStatus(); 
	tempvalholder tmap = new tempvalholder();
    ClusterStatus clusterStatus = taskTrackerManager.getClusterStatus();
    final int numTaskTrackers = clusterStatus.getTaskTrackers();
    final int clusterMapCapacity = clusterStatus.getMaxMapTasks();
    final int clusterReduceCapacity = clusterStatus.getMaxReduceTasks();
	int availableMapSlots = 0;
	/*int average = 0, sum=0, dtaverage = 0, dtsum=0; 
	float duaverage = 0, dusum=0, cusum = 0, cuaverage=0;
	 int count; float sum_cpusage =0;
	
	float davg=0, cpuavg=0;

	clusterStatus.tempmap.put(taskTrackerStatus.trackerName, taskTrackerStatus.getResourceStatus().cpuTemp);
	clusterStatus.dtempmap.put(taskTrackerStatus.trackerName, taskTrackerStatus.getResourceStatus().diskTemp);
	clusterStatus.dusagemap.put(taskTrackerStatus.trackerName, taskTrackerStatus.getResourceStatus().diskUsage);
	clusterStatus.cpuusagemap.put(taskTrackerStatus.trackerName, taskTrackerStatus.getResourceStatus().cpuUsage);
		
	Collection c  =  clusterStatus.tempmap.values();
	Iterator itr = c.iterator();
	Set st = clusterStatus.tempmap.keySet();
	Iterator itrkeys = st.iterator();

	
	//clusterStatus.cprise.put(taskTrackerStatus.trackerName, taskTrackerStatus.getResourceStatus().crise);
    //LOG.info("Tigers new init "+taskTrackerStatus.getResourceStatus().cinittemp+" rise "+taskTrackerStatus.getResourceStatus().crise);
	

	Collection disktc  =  clusterStatus.dtempmap.values();
        Iterator dtempitr = disktc.iterator();
        Set disktst = clusterStatus.dtempmap.keySet();
        Iterator dtitrkeys = disktst.iterator();
        while(dtempitr.hasNext())
                {
                  LOG.info("Tigers new dtemp "+dtempitr.next()+" "+dtitrkeys.next()+"size  "+clusterStatus.dtempmap.size());
				 //static_scheduler
		 if(taskTrackerStatus.getResourceStatus().diskTemp!=0 && taskTrackerStatus.getResourceStatus().dinittemp==-1)
		  	{
		  	taskTrackerStatus.getResourceStatus().dinittemp = taskTrackerStatus.getResourceStatus().diskTemp;
			clusterStatus.dinit.put(taskTrackerStatus.trackerName, taskTrackerStatus.getResourceStatus().dinittemp);
		  	}
		  else
		  	{
		  	taskTrackerStatus.getResourceStatus().drise = taskTrackerStatus.getResourceStatus().diskTemp - taskTrackerStatus.getResourceStatus().dinittemp;
			clusterStatus.drise.put(taskTrackerStatus.trackerName, taskTrackerStatus.getResourceStatus().drise);
		  	}
				  
                }

	Collection diskuc  =  clusterStatus.dusagemap.values();
        Iterator dutilitr = diskuc.iterator();
        Set diskust = clusterStatus.dusagemap.keySet();
        Iterator duitrkeys = diskust.iterator();
        while(dutilitr.hasNext())
                {
                  LOG.info("Tigers new dutil "+dutilitr.next()+" "+duitrkeys.next()+"size  "+clusterStatus.dusagemap.size());
                }

	Collection cpuuc  =  clusterStatus.cpuusagemap.values();
        Iterator cutilitr = cpuuc.iterator();
        Set cpuust = clusterStatus.cpuusagemap.keySet();
        Iterator cpuuitrkeys = cpuust.iterator();
        while(cutilitr.hasNext())
                {
                  LOG.info("Tigers new cpuutil "+cutilitr.next()+" "+cpuuitrkeys.next()+"size  "+clusterStatus.cpuusagemap.size());
                }

    
	
	for(Integer value : clusterStatus.tempmap.values())
		sum=sum+value;

	
	for(Integer dtvalue : clusterStatus.dtempmap.values())
			dtsum=dtsum+dtvalue;

	for(Float duvalue : clusterStatus.dusagemap.values())
			dusum=dusum+duvalue;

	for(Float cpuuvalue : clusterStatus.cpuusagemap.values())
                        cusum=cusum+cpuuvalue;
	
	LOG.info("sum is "+sum);
	LOG.info("dtsum is "+dtsum);
	LOG.info("dusum is "+dusum);
	LOG.info("cpusum is "+cusum);

	average = sum/clusterStatus.tempmap.size();
	dtaverage = dtsum/clusterStatus.dtempmap.size();
	duaverage = dusum/clusterStatus.dusagemap.size();
	cuaverage  = cusum/clusterStatus.cpuusagemap.size();

    cpuavg = sum/clusterStatus.tempmap.size();
	davg = dtsum/clusterStatus.dtempmap.size();
	
	taskTrackerStatus.setCTempAvg(cpuavg);
	taskTrackerStatus.setDTempAvg(davg);
	LOG.info("Is it going to static schedule? "+taskTrackerStatus.getCTempAvg()+" "+taskTrackerStatus.getDTempAvg());*/
	
//	LOG.info("Tigers: JobQueueTaskScheduler assignTasks with CPU temp "+taskTrackerStatus.getResourceStatus().cpuTemp);
//	LOG.info("Tigers: JobQueueTaskScheduler assignTasks with Disk temp "+taskTrackerStatus.getResourceStatus().diskTemp);

//	int temp = ((int)clusterStatus.tempmap.get(taskTrackerStatus.trackerName));//.intValue();
	
//	LOG.info("Tigers: JobQueueTaskScheduler assignTasks 2 with CPU temp  "+temp);
//	temp = 100;
//	clusterStatus.tempmap.put(taskTrackerStatus.trackerName, temp);

//	LOG.info("Tigers: JobQueueTaskScheduler assignTasks 2 with CPU temp  "+clusterStatus.tempmap.get(taskTrackerStatus.trackerName));
	
    Collection<JobInProgress> jobQueue =
      jobQueueJobInProgressListener.getJobQueue();

    //
    // Get map + reduce counts for the current tracker.
    //
    final int trackerMapCapacity = taskTrackerStatus.getMaxMapSlots();
    final int trackerReduceCapacity = taskTrackerStatus.getMaxReduceSlots();
    final int trackerRunningMaps = taskTrackerStatus.countMapTasks();
    final int trackerRunningReduces = taskTrackerStatus.countReduceTasks();

    // Assigned tasks
    List<Task> assignedTasks = new ArrayList<Task>();

    //

    // Compute (running + pending) map and reduce task numbers across pool
    //
    int remainingReduceLoad = 0;
    int remainingMapLoad = 0;
    synchronized (jobQueue) {
      for (JobInProgress job : jobQueue) {
        if (job.getStatus().getRunState() == JobStatus.RUNNING) {
			job.profile.cputemp = taskTrackerStatus.getCTempAvg();
			job.profile.dtemp = taskTrackerStatus.getDTempAvg();
			job.profile.dutil = taskTrackerStatus.getDiskUtilAvg();
			job.profile.cpuutil = taskTrackerStatus.getCpuUtilAvg();
			
			if(taskTrackerStatus.getResourceStatus().cpuUsage != -1)
					{
						tmap.avg=tmap.sum=0.0f;
						tmap.count=0;
					 	Collection<Float> values  = tmap.profCpuUsageMap.get(taskTrackerStatus.trackerName);
				        if(values==null)
				        {
                			values = new ArrayList<Float>();
			                tmap.profCpuUsageMap.put(taskTrackerStatus.trackerName,values);
        				}
				         values.add(taskTrackerStatus.getResourceStatus().cpuUsage);

    					 Collection<Float> array = tmap.profCpuUsageMap.get(taskTrackerStatus.trackerName);

				        Iterator itr1 = array.iterator();
						 while(itr1.hasNext())
						 	{
                				//LOG.info("The values are  for "+taskTrackerStatus.trackerName+" "+itr1.next()+" ");
								tmap.count++;
								tmap.sum += Float.parseFloat(""+itr1.next());
						 	}
						tmap.avg = tmap.sum/tmap.count;
						clusterStatus.avgcpusage.put(taskTrackerStatus.trackerName,tmap.avg);
					}
			
			if(taskTrackerStatus.getResourceStatus().diskUsage!= -1)
					{
						tmap.avg=tmap.sum=0.0f;
						tmap.count=0;					
					 	Collection<Float> values  = tmap.profDiskUsageMap.get(taskTrackerStatus.trackerName);
				        if(values==null)
				        {
                			values = new ArrayList<Float>();
			                tmap.profDiskUsageMap.put(taskTrackerStatus.trackerName,values);
        				}
				         values.add(taskTrackerStatus.getResourceStatus().diskUsage);

    					Collection<Float> array = tmap.profDiskUsageMap.get(taskTrackerStatus.trackerName);
				        Iterator itr1 = array.iterator();
						 while(itr1.hasNext())
						 	{
                				//LOG.info("The values are  for "+taskTrackerStatus.trackerName+" "+itr1.next()+" ");
								tmap.count++;
								tmap.sum += Float.parseFloat(""+itr1.next());
						 	}
						tmap.avg = tmap.sum/tmap.count;
						clusterStatus.avgdusage.put(taskTrackerStatus.trackerName,tmap.avg);
					}
			
          remainingMapLoad += (job.desiredMaps() - job.finishedMaps());
          if (job.scheduleReduces()) {
            remainingReduceLoad += 
              (job.desiredReduces() - job.finishedReduces());
          }
        }
      }
    }

    // Compute the 'load factor' for maps and reduces
    double mapLoadFactor = 0.0;
    if (clusterMapCapacity > 0) {
      mapLoadFactor = (double)remainingMapLoad / clusterMapCapacity;
    }
    double reduceLoadFactor = 0.0;
    if (clusterReduceCapacity > 0) {
      reduceLoadFactor = (double)remainingReduceLoad / clusterReduceCapacity;
    }
        
    //
    // In the below steps, we allocate first map tasks (if appropriate),
    // and then reduce tasks if appropriate.  We go through all jobs
    // in order of job arrival; jobs only get serviced if their 
    // predecessors are serviced, too.
    //

    //
    // We assign tasks to the current taskTracker if the given machine 
    // has a workload that's less than the maximum load of that kind of
    // task.
    // However, if the cluster is close to getting loaded i.e. we don't
    // have enough _padding_ for speculative executions etc., we only 
    // schedule the "highest priority" task i.e. the task from the job 
    // with the highest priority.
    //
    
    final int trackerCurrentMapCapacity = 
      Math.min((int)Math.ceil(mapLoadFactor * trackerMapCapacity), 
                              trackerMapCapacity);
	
//	if(average < taskTrackerStatus.getResourceStatus().cpuTemp || dtaverage < taskTrackerStatus.getResourceStatus().diskTemp)
//	{
//    	  availableMapSlots = (trackerCurrentMapCapacity - trackerRunningMaps)/2;
//	  LOG.info("Tigers: reducing the load "+taskTrackerStatus.trackerName);
//	}

//	else

	availableMapSlots = trackerCurrentMapCapacity - trackerRunningMaps;
//	LOG.info("available mapslots "+availableMapSlots+" max tracker slots "+taskTrackerStatus.getMaxMapSlots()+ " trackerCurrentMapCapacity  "+trackerCurrentMapCapacity+" trackerRunningMaps  "+trackerRunningMaps);
	
    boolean exceededMapPadding = false;
    if (availableMapSlots > 0) {
      exceededMapPadding = 
        exceededPadding(true, clusterStatus, trackerMapCapacity);
    }
    
    int numLocalMaps = 0;
    int numNonLocalMaps = 0;
    scheduleMaps:
    for (int i=0; i < availableMapSlots; ++i) {
      synchronized (jobQueue) {
        for (JobInProgress job : jobQueue) {
			LOG.info(job.profile.cputemp);
          if (job.getStatus().getRunState() != JobStatus.RUNNING) {
            continue;
          }

          Task t = null;
          
          // Try to schedule a node-local or rack-local Map task
          t = 
            job.obtainNewNodeOrRackLocalMapTask(taskTrackerStatus, 
                numTaskTrackers, taskTrackerManager.getNumberOfUniqueHosts());
          if (t != null) {
            assignedTasks.add(t);
            ++numLocalMaps;
            
            // Don't assign map tasks to the hilt!
            // Leave some free slots in the cluster for future task-failures,
            // speculative tasks etc. beyond the highest priority job
            if (exceededMapPadding) {
              break scheduleMaps;
            }
           
            // Try all jobs again for the next Map task 
            break;
          }
          
          // Try to schedule a node-local or rack-local Map task
          t = 
            job.obtainNewNonLocalMapTask(taskTrackerStatus, numTaskTrackers,
                                   taskTrackerManager.getNumberOfUniqueHosts());
          
          if (t != null) {
            assignedTasks.add(t);
            ++numNonLocalMaps;
            
            // We assign at most 1 off-switch or speculative task
            // This is to prevent TaskTrackers from stealing local-tasks
            // from other TaskTrackers.
            break scheduleMaps;
          }
        }
      }
    }
    int assignedMaps = assignedTasks.size();

    //
    // Same thing, but for reduce tasks
    // However we _never_ assign more than 1 reduce task per heartbeat
    //
    final int trackerCurrentReduceCapacity = 
      Math.min((int)Math.ceil(reduceLoadFactor * trackerReduceCapacity), 
               trackerReduceCapacity);
    final int availableReduceSlots = 
      Math.min((trackerCurrentReduceCapacity - trackerRunningReduces), 1);
    boolean exceededReducePadding = false;
    if (availableReduceSlots > 0) {
      exceededReducePadding = exceededPadding(false, clusterStatus, 
                                              trackerReduceCapacity);
      synchronized (jobQueue) {
        for (JobInProgress job : jobQueue) {
          if (job.getStatus().getRunState() != JobStatus.RUNNING ||
              job.numReduceTasks == 0) {
            continue;
          }

          Task t = 
            job.obtainNewReduceTask(taskTrackerStatus, numTaskTrackers, 
                                    taskTrackerManager.getNumberOfUniqueHosts()
                                    );
          if (t != null) {
            assignedTasks.add(t);
            break;
          }
          
          // Don't assign reduce tasks to the hilt!
          // Leave some free slots in the cluster for future task-failures,
          // speculative tasks etc. beyond the highest priority job
          if (exceededReducePadding) {
            break;
          }
        }
      }
    }
    
    if (LOG.isDebugEnabled()) {
      LOG.debug("Task assignments for " + taskTrackerStatus.getTrackerName() + " --> " +
                "[" + mapLoadFactor + ", " + trackerMapCapacity + ", " + 
                trackerCurrentMapCapacity + ", " + trackerRunningMaps + "] -> [" + 
                (trackerCurrentMapCapacity - trackerRunningMaps) + ", " +
                assignedMaps + " (" + numLocalMaps + ", " + numNonLocalMaps + 
                ")] [" + reduceLoadFactor + ", " + trackerReduceCapacity + ", " + 
                trackerCurrentReduceCapacity + "," + trackerRunningReduces + 
                "] -> [" + (trackerCurrentReduceCapacity - trackerRunningReduces) + 
                ", " + (assignedTasks.size()-assignedMaps) + "]");
    }

    return assignedTasks;
  }

  private boolean exceededPadding(boolean isMapTask, 
                                  ClusterStatus clusterStatus, 
                                  int maxTaskTrackerSlots) { 
    int numTaskTrackers = clusterStatus.getTaskTrackers();
    int totalTasks = 
      (isMapTask) ? clusterStatus.getMapTasks() : 
        clusterStatus.getReduceTasks();
    int totalTaskCapacity = 
      isMapTask ? clusterStatus.getMaxMapTasks() : 
                  clusterStatus.getMaxReduceTasks();

    Collection<JobInProgress> jobQueue =
      jobQueueJobInProgressListener.getJobQueue();

    boolean exceededPadding = false;
    synchronized (jobQueue) {
      int totalNeededTasks = 0;
      for (JobInProgress job : jobQueue) {
        if (job.getStatus().getRunState() != JobStatus.RUNNING ||
            job.numReduceTasks == 0) {
          continue;
        }

        //
        // Beyond the highest-priority task, reserve a little 
        // room for failures and speculative executions; don't 
        // schedule tasks to the hilt.
        //
        totalNeededTasks += 
          isMapTask ? job.desiredMaps() : job.desiredReduces();
        int padding = 0;
        if (numTaskTrackers > MIN_CLUSTER_SIZE_FOR_PADDING) {
          padding = 
            Math.min(maxTaskTrackerSlots,
                     (int) (totalNeededTasks * padFraction));
        }
        if (totalTasks + padding >= totalTaskCapacity) {
          exceededPadding = true;
          break;
        }
      }
    }

    return exceededPadding;
  }

  @Override
  public synchronized Collection<JobInProgress> getJobs(String queueName) {
    return jobQueueJobInProgressListener.getJobQueue();
  }  
}

