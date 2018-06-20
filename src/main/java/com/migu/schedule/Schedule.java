package com.migu.schedule;


import com.migu.schedule.constants.ReturnCodeKeys;
import com.migu.schedule.info.Task;
import com.migu.schedule.info.TaskInfo;

import java.util.*;

/*
*类名和方法不能修改
 */
public class Schedule {

    //服务节点列表
    private List<Integer> nodeIdList = null;

    //任务列表
    private List<Task> taskList = null;

    //任务信息列表
    private Map<Integer,List<TaskInfo>> taskInfoMap = null;

    //运行任务列表
    private Map<Integer,List<Integer>> runTimeTaskMap = null;

    //阈值
    private int threshold = -1;

    Comparator<TaskInfo> comp = new Comparator<TaskInfo>(){
        public int compare(TaskInfo task1, TaskInfo task2) {
            return (task1.getTaskId()-task1.getTaskId());
        }
    };

    public int init() {
        if(nodeIdList != null) {
            nodeIdList = null;
        }
        if(taskList != null) {
            taskList = null;
        }
        if(taskInfoMap != null) {
            taskInfoMap = null;
        }
        if(runTimeTaskMap != null) {
            runTimeTaskMap = null;
        }
        nodeIdList = new ArrayList<Integer>();
        taskList = new ArrayList<Task>();
        taskInfoMap = new HashMap<Integer,List<TaskInfo>>();
        runTimeTaskMap = new HashMap<Integer, List<Integer>>();
        return ReturnCodeKeys.E001;
    }

    public int registerNode(int nodeId) {
        if(nodeId <= 0) {
            return ReturnCodeKeys.E004;
        }
        if(nodeIdList.contains(nodeId)) {
            return ReturnCodeKeys.E005;
        }
        nodeIdList.add(nodeId);
        return ReturnCodeKeys.E003;
    }

    public int unregisterNode(int nodeId) {
        if(nodeId <= 0) {
            return ReturnCodeKeys.E004;
        }
        if(nodeIdList.contains(nodeId)) {
            nodeIdList.remove(nodeId);
            return ReturnCodeKeys.E006;
        }
        return ReturnCodeKeys.E007;
    }


    public int addTask(int taskId, int consumption) {
        if(taskId <= 0) {
            return ReturnCodeKeys.E009;
        }
        for(Task t : taskList) {
            if(taskId == t.getTaskId()) {
                return ReturnCodeKeys.E010;
            }
        }
        Task task = new Task(taskId,consumption);
        taskList.add(task);
        return ReturnCodeKeys.E008;
    }


    public int deleteTask(int taskId) {
        if(taskId <= 0) {
            return ReturnCodeKeys.E009;
        }
        for(Task t : taskList) {
            if(taskId == t.getTaskId()) {
                taskList.remove(t);
                return ReturnCodeKeys.E011;
            }
        }
        return ReturnCodeKeys.E012;
    }


    public int scheduleTask(int threshold) {
        if(threshold <= 0) {
            return ReturnCodeKeys.E002;
        }
        if(taskList.isEmpty()) {
            return ReturnCodeKeys.E014;
        }
        boolean flag = false;
        this.threshold = threshold;
        //创建临时队列用来调度
        List<Task> tempTaskQueue = new ArrayList<Task>();
        for(Task task : taskList) {
            tempTaskQueue.add(task);
        }
        //添加任务信息列表数据
        for(Integer in : nodeIdList) {
            List<TaskInfo> tInf = new ArrayList<TaskInfo>();
            taskInfoMap.put(in, tInf);
        }
        while(!flag && tempTaskQueue.size() > 0){
            for(Task task : tempTaskQueue) {
                int tempNodeId = -1;
                for(Integer nid : nodeIdList){
                    List<TaskInfo> TIF = taskInfoMap.get(nid);
                    if(TIF==null){
                        tempNodeId = nid;
                    }
                }
                List<TaskInfo> listTINF = taskInfoMap.get(tempNodeId);
                TaskInfo TINF = new TaskInfo();
                TINF.setTaskId(task.getTaskId());
                TINF.setNodeId(task.getTaskId());
                listTINF.add(TINF);
                tempTaskQueue.remove(new Integer(task.getTaskId()));
                for(Task t : taskList) {
                    if(t.getTaskId() == task.getTaskId()) {
                        int co = t.getConsumption();
                        List<Integer> list = runTimeTaskMap.get(co);
                        if (list == null) {
                            list = new ArrayList<Integer>();
                            runTimeTaskMap.put(co, list);
                        }
                        break;
                    }
                }
                flag = isOptimal(tempNodeId);
                break;
            }
            if(tempTaskQueue.size()==0 && !flag) {
                return ReturnCodeKeys.E014;
            }
        }
        for (Integer runT : runTimeTaskMap.keySet()){
            List<Integer> runList = runTimeTaskMap.get(runT);
            if(runList.size()>1){
                List<TaskInfo> tsk = new ArrayList<TaskInfo>();
                for(Integer nodeId:taskInfoMap.keySet()){
                    List<TaskInfo> taskInfos = taskInfoMap.get(nodeId);
                    for(TaskInfo ti:taskInfos){
                        if(runList.contains(ti.getTaskId())){
                            tsk.add(ti);
                        }
                    }
                }
                for(int i=0;i<tsk.size();i++){
                    TaskInfo ti = tsk.get(i);
                    ti.setTaskId(runList.get(i));
                }
            }
        }
        return ReturnCodeKeys.E013;
    }

    private boolean isOptimal(int nodeId){
        int coms = -1;
        for(Task t : taskList) {
            coms += t.getConsumption();
        }
        for(Integer nid:nodeIdList){
            if(!nid.equals(nodeId)){
                int nl = 0;
                if(taskInfoMap.get(nid)==null){
                    nl=0;
                }else{
                    nl = coms;
                }
                if(Math.abs(nl-coms)>this.threshold) return false;
            }
        }
        return true;
    }

    public int queryTaskStatus(List<TaskInfo> tasks) {
        if(tasks == null) {
            tasks = new ArrayList<TaskInfo>();
        }
        for(Integer i : taskInfoMap.keySet()){
            if(i == -1) {
                continue;
            }
            tasks.addAll(taskInfoMap.get(i));
        }
        if(tasks == null ) {
            return ReturnCodeKeys.E016;
        }
        Collections.sort(tasks,comp);
        return ReturnCodeKeys.E015;
    }

}
