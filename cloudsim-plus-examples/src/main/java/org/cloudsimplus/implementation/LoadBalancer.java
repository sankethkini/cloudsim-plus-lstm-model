package org.cloudsimplus.implementation;

import org.cloudbus.cloudsim.brokers.DatacenterBroker;
import org.cloudbus.cloudsim.cloudlets.Cloudlet;
import org.cloudbus.cloudsim.schedulers.cloudlet.CloudletSchedulerSpaceShared;
import org.cloudbus.cloudsim.vms.Vm;
import org.cloudbus.cloudsim.vms.VmSimple;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.cloudsimplus.implementation.NormalCase.HOST_MIPS;
import static org.cloudsimplus.implementation.NormalCase.VM_PES;

public class LoadBalancer {

    private DatacenterBroker broker;
    private Map<Integer,Integer> vmMap;
    private static final int MAX_CLOUDLET_PER_VM=25;

    public static int vmID=0;

    LoadBalancer(DatacenterBroker broker){
        this.broker=broker;
        this.vmMap=new HashMap<>();
    }

    public void submitWorkload(List<Cloudlet> cloudletList){
        List<Vm> execList = broker.getVmExecList();
        List<Vm> waitingList = broker.getVmWaitingList();
        List<Vm> newVms=new ArrayList<>();
        for(Cloudlet cloudlet:cloudletList){
            if(getVmForCloudlet(cloudlet,execList)){
                continue;
            }
            if(getVmForCloudlet(cloudlet,waitingList)){
                continue;
            }
            if(getVmForCloudlet(cloudlet,newVms)){
                continue;
            }
            Vm vm=createVM();
            vm.setSubmissionDelay(90);
            cloudlet.setVm(vm);
            vmMap.put((int)vm.getId(),vmMap.get((int)vm.getId())+1);
            newVms.add(vm);

        }
        if(newVms.size()!=0){
            broker.submitVmList(newVms);
        }
        broker.submitCloudletList(cloudletList);
    }

    public void postWork(Cloudlet cl){
        int id=(int) cl.getVm().getId();
        vmMap.put(id,vmMap.get(id)-1);
    }

    private boolean getVmForCloudlet(Cloudlet cloudlet,List<Vm> vmList){
        if(vmList==null || vmList.size()==0)return false;
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

    private Vm createVM(){
        Vm vm=new VmSimple(HOST_MIPS, VM_PES,new CloudletSchedulerSpaceShared())
            .setRam(512).setBw(1000).setSize(10_000);
        vm.setId(vmID++);
        vmMap.put((int)vm.getId(),0);
        return vm;
    }

    public void horizontalScaling(Integer cls){
        List<Vm> vmList=new ArrayList<>();
        Integer capacity = getCurrentCapacity();
        if(capacity>cls)return;
        int remaining= cls-capacity;
        int requiredVms = Math.round(remaining/MAX_CLOUDLET_PER_VM);
        for(int i=0;i<requiredVms;i++){
            Vm vm = createVM();
            vm.setSubmissionDelay(90);
            vmList.add(vm);
        }
        broker.submitVmList(vmList);
    }

    private Integer getCurrentCapacity(){
        Integer res=0;
        for(Integer val:vmMap.values()){
            if(val<MAX_CLOUDLET_PER_VM){
                res=res+(MAX_CLOUDLET_PER_VM-val);
            }
        }
        return res;
    }



}
