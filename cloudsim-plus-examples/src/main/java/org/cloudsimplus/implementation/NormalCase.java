package org.cloudsimplus.implementation;

import ch.qos.logback.classic.Level;
import org.apache.commons.lang3.ObjectUtils;
import org.cloudbus.cloudsim.brokers.DatacenterBroker;
import org.cloudbus.cloudsim.brokers.DatacenterBrokerSimple;
import org.cloudbus.cloudsim.cloudlets.Cloudlet;
import org.cloudbus.cloudsim.cloudlets.CloudletSimple;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.Simulation;
import org.cloudbus.cloudsim.datacenters.Datacenter;
import org.cloudbus.cloudsim.datacenters.DatacenterSimple;
import org.cloudbus.cloudsim.hosts.Host;
import org.cloudbus.cloudsim.hosts.HostSimple;
import org.cloudbus.cloudsim.resources.Pe;
import org.cloudbus.cloudsim.resources.PeSimple;
import org.cloudbus.cloudsim.schedulers.cloudlet.CloudletSchedulerSpaceShared;
import org.cloudbus.cloudsim.utilizationmodels.UtilizationModelDynamic;
import org.cloudbus.cloudsim.vms.Vm;
import org.cloudbus.cloudsim.vms.VmSimple;
import org.cloudsimplus.builders.tables.CloudletsTableBuilder;
import org.cloudsimplus.listeners.CloudletVmEventInfo;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.*;
import org.cloudsimplus.util.Log;

public class NormalCase {

    public static final int NUM_HOST = 20;
    public static final int HOST_PES = 32;
    public static final int INITIAL_VMS=2;
    public static final int VM_PES=8;
    public static final int PE_MIPS=1000;
    public static final int CLOUDLET_LENGTH_PER_PES =5000;
    public static final int CLOUDLET_PES=2;
    public static final int  HOST_MIPS = 1000;
    private static final int  HOST_RAM = 2048000;
    private static final long HOST_BW = 1000000000;
    private static final long HOST_STORAGE = 1000000000;

    private static final int CLOUDLET_LENGTH = 10000;

    private static final int INTERVAL=100;

    private static final int CLOUDLET_INTERVAL=300;

    private static final int MAX_CLOUDLET_PER_VM=24;

    private static boolean FirstRound=true;

    private static List<Cloudlet> cloudlets;

    private static List<Vm> vms;

    private final CloudSim simulation;
    private final Datacenter datacenter0;

    private static double nextTime=0;

    private int timeIndex=2;
    private final DatacenterBroker broker;

    private static double previousClock;

    private static List<Integer> input;

    Map<Integer,Integer> vmMap;

    public static int vmID=0;
    public static int CloudletID=0;

    private LoadBalancer loadBalancer;

    public static void main(String[] args) {
       try {
           new NormalCase();
       } catch (Exception e){
           System.out.println(e.getMessage());
       }
    }

    public NormalCase() throws Exception{
        Log.setLevel(Level.ERROR);
        simulation = new CloudSim();
        datacenter0 = createDatacenter(simulation);
        broker=new DatacenterBrokerSimple(simulation);
        loadBalancer = new  LoadBalancer(broker);
        incrementTime();
        input=readInputs();
        List<Cloudlet> cloudletList=getNewCloudlets();
        loadBalancer.submitWorkload(cloudletList);
        broker.setVmDestructionDelay(60);

        simulation.startSync();
        while (simulation.isRunning()){
            simulation.runFor(INTERVAL);
            if(timeIndex>=input.size()){
                continue;
            }
            if((simulation.clock()%CLOUDLET_INTERVAL==0 && simulation.clock()!=0.0) || simulation.clock()>=nextTime){
                double cl =simulation.clock();
                simulation.pause();
                incrementTime();
                printVmCpuUtilization();
                List<Cloudlet> cls= getNewCloudlets();
                for(Cloudlet c:cls){
                    c.setSubmitedTime(cl);
                }
                loadBalancer.submitWorkload(cls);
                simulation.resume();
            }
        }

        final List<Cloudlet> finishedCloudlets = broker.getCloudletCreatedList();
        finishedCloudlets.sort(Comparator.comparingLong(cloudlet -> cloudlet.getId()));
        new CloudletsTableBuilder(finishedCloudlets).build();
    }

    private List<Cloudlet> createCloudletForGivenMIPS(int mips){
        List<Cloudlet> cloudletList=new ArrayList<>();
        int mainCount=mips/ CLOUDLET_LENGTH;
        int extra=mips-mainCount* CLOUDLET_LENGTH;
        for(int i=0;i<mainCount;i++){
            Cloudlet cl=createCloudlet(CLOUDLET_LENGTH);
            cloudletList.add(cl);
        }
        if(extra!=0){
            Cloudlet cl=createCloudlet(extra);
            cloudletList.add(cl);
        }
        return cloudletList;
    }
    private Datacenter createDatacenter(Simulation simulation){
        List<Host> hosts=new ArrayList<>();
        for(int i=0;i<NUM_HOST;i++){
            hosts.add(createHost());
        }
        Datacenter datacenter=new DatacenterSimple(simulation,hosts);
        return datacenter;
    }

    private Host createHost(){
        List<Pe> peList = new ArrayList<>();
        for(int i=0;i<HOST_PES;i++){
            peList.add(new PeSimple(HOST_MIPS));
        }
        Host host=new HostSimple(HOST_RAM, HOST_BW, HOST_STORAGE,peList);
        return host;
    }

    private Cloudlet createCloudlet(double length){
        final var utilizationModel = new UtilizationModelDynamic(0.1);
        Cloudlet cloudlet;
        if(length== CLOUDLET_LENGTH_PER_PES *2){
             cloudlet = new CloudletSimple(CLOUDLET_LENGTH_PER_PES, CLOUDLET_PES, utilizationModel)
                .setSizes(1024);
        } else if (length> CLOUDLET_LENGTH_PER_PES){
            cloudlet = new CloudletSimple((int)length/2,CLOUDLET_PES,utilizationModel)
                .setSizes(1024);
        } else {
            cloudlet =new CloudletSimple((int)length,1,utilizationModel)
                .setSizes(1024);
        }
        cloudlet.addOnFinishListener(this::onCloudletFinishListener);
        cloudlet.setId(CloudletID++);
        return cloudlet;
    }

    private List<Integer> readInputs() throws Exception{
        List<Integer> output=new ArrayList<>();
        BufferedReader br = new BufferedReader(new FileReader("/home/admin1/reasearch/dataset.csv"));
        String line;
        br.readLine();
        line=br.readLine();
        int c=0;
        while (line!=null && c<7){
            String[] data=line.split(",");
            Float out=Float.parseFloat(data[3]);
            output.add(Math.round(out));
            line=br.readLine();
            c++;
        }
        return output;
    }


    private List<Cloudlet> getInitialCLS(){
        List<Cloudlet> cloudletList = createCloudletForGivenMIPS(input.get(timeIndex));
        timeIndex++;
        return cloudletList;
    }

    private List<Cloudlet> getNewCloudlets(){
        List<Cloudlet> cloudletList = createCloudletForGivenMIPS(input.get(timeIndex));
        timeIndex++;
        return cloudletList;
    }



    private boolean getVmForCloudlet(Cloudlet cloudlet,List<Vm> vmList){
        for(Vm vm:vmList){
            int vmId = (int) vm.getId();
            if(vmMap.get(vmId)<MAX_CLOUDLET_PER_VM){
                cloudlet.setVm(vm);
                vmMap.put(vmId,vmMap.getOrDefault(vmId,0)+1);
                return true;
            }
        }
        return false;
    }

    private void printVmCpuUtilization() {
        if(simulation.clock() == previousClock ||
            Math.round(simulation.clock()) % INTERVAL != 0 ||
            broker.getVmExecList().isEmpty())
        {
            return;
        }
        previousClock = simulation.clock();
        System.out.printf("\t\tVM CPU utilization for Time %.0f%n", simulation.clock());
        for (final Vm vm : broker.getVmExecList()) {
            System.out.printf(" Vm %5d |", vm.getId());
        }
        System.out.println();
        for (final Vm vm : broker.getVmExecList()) {
            System.out.printf(" %7.0f%% |", vm.getCpuPercentUtilization()*100);
        }
        System.out.printf("%n%n");
    }

    private void incrementTime(){
        nextTime+=CLOUDLET_INTERVAL;
    }

    private void onCloudletFinishListener(CloudletVmEventInfo eventInfo){
       loadBalancer.postWork(eventInfo.getCloudlet());
    }

    private void addCLToMap(){
        for(Cloudlet cl:cloudlets){
            Integer id=(int) cl.getVm().getId();
            if(!cl.isFinished() && id!=-1){
                if(!vmMap.containsKey(id)){
                    vmMap.put(id,0);
                }
                vmMap.put(id,vmMap.getOrDefault(id,0)+1);
            }
        }
    }


}
