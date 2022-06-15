package org.cloudsimplus.implementation;

import ch.qos.logback.classic.Level;
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
import org.cloudbus.cloudsim.utilizationmodels.UtilizationModelDynamic;
import org.cloudbus.cloudsim.vms.Vm;
import org.cloudsimplus.builders.tables.CloudletsTableBuilder;
import org.cloudsimplus.listeners.CloudletVmEventInfo;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;
import org.cloudsimplus.util.Log;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class MLCase {

    private final CloudSim simulation;

    private final Datacenter datacenter;
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

    private static  int VM_INTERVAL=220;

    private static final int CLOUDLET_INTERVAL=300;

    private static final int MAX_CLOUDLET_PER_VM=24;

    private  double nextTime=0;

    private  double nextVmTime=0;


    private LoadBalancer loadBalancer;

    private final DatacenterBroker broker;

    private List<Integer> input;

    private static double previousClock;

    private int timeIndex=2;

    private List<Integer> last3Data;

    private RestTemplate restTemplate;

    private RestClient restClient;

    public static int CloudletID=0;

    public static void main(String[] args) {
        try {
            new MLCase();
        } catch (Exception e){
            System.out.println(e.getMessage());
        }
    }

    private MLCase() throws Exception{
        Log.setLevel(Level.ERROR);
        this.restTemplate=new RestTemplate();
        this.restClient=new RestClient(this.restTemplate);
        simulation=new CloudSim();
        datacenter=createDatacenter(simulation);
        broker=new DatacenterBrokerSimple(simulation);
        loadBalancer=new LoadBalancer(broker);
        broker.setVmDestructionDelay(60);
        incrementTime();
        incrementVmTime();
        input=readInputs();
        List<Cloudlet> cloudletList=getNewCloudlets();
        loadBalancer.submitWorkload(cloudletList);
        last3Data=new ArrayList<>();
        last3Data.add(1229569);
        last3Data.add(1211321);
        last3Data.add(1206634);
        simulation.startSync();
        while (simulation.isRunning()){
            simulation.runFor(INTERVAL);
            if(timeIndex>=input.size()){
                continue;
            }
            if((simulation.clock()%VM_INTERVAL==0 && simulation.clock()!=0.0) || simulation.clock()>VM_INTERVAL){
                VM_INTERVAL+=CLOUDLET_INTERVAL;
                double cl = simulation.clock();
                simulation.pause();
                incrementVmTime();
                printVmCpuUtilization();
                Integer possibleCPU = getFutureWorkload();
                Integer possibleCLs= getPossibleCloudletFromCpu(possibleCPU);
                loadBalancer.horizontalScaling(possibleCLs);
                simulation.resume();
            }

            if((simulation.clock()%CLOUDLET_INTERVAL==0 && simulation.clock()!=0.0) || simulation.clock()>nextTime){
                double cl = simulation.clock();
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

    private void incrementTime(){
        nextTime+=CLOUDLET_INTERVAL;
    }

    private void incrementVmTime(){
        nextVmTime+=VM_INTERVAL;
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

    private List<Cloudlet> getNewCloudlets(){
        List<Cloudlet> cloudletList = createCloudletForGivenMIPS(input.get(timeIndex));
        timeIndex++;
        return cloudletList;
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

    private void onCloudletFinishListener(CloudletVmEventInfo eventInfo){
        loadBalancer.postWork(eventInfo.getCloudlet());
    }

    private void printVmCpuUtilization() {
        if(simulation.clock() == previousClock ||
            Math.round(simulation.clock()) % CLOUDLET_INTERVAL != 0 ||
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

    private RequestToServer buildRequest(){
        return new RequestToServer(
            Integer.toString(last3Data.get(0)),
            Integer.toString(last3Data.get(1)),
            Integer.toString(last3Data.get(2)));
    }

    private Integer getFutureWorkload(){
        RequestToServer request=buildRequest();
        ResponseEntity<ResponseFromServer> response= this.restClient.getCPUUsage(request);
        Integer res= Math.round(Float.parseFloat(response.getBody().getRes()));
        last3Data.set(0,last3Data.get(1));
        last3Data.set(1,last3Data.get(2));
        last3Data.set(2,res);
        return res;
    }

    private Integer getPossibleCloudletFromCpu(Integer mips){
        return mips/CLOUDLET_LENGTH;
    }

}
