# broadcast network simulator
# each frame, advance simulation by x ms
# each node sends every y ms (with some variance)

# nodes exist in groups, and each individual nodes broadcast is only received by nodes in neighboring groups
#   a group is defined by the collection of nodes that receive each others broadcasts directly
#   some nodes are in multiple groups
#       - enables information to be copied across the network despite each node acting only locally within its group
# some groups may be large, some may be small
# some nodes may only be present in a single group, while other nodes may be part of many groups
# a router would be a node that is present in 2+ groups for the express purpose of connecting them
# [0, 1, 2]                            # i.e. a room with two devices and a router
# [2, 3, 4, 5, 6, ..., 10]             # an apartment full of devices and a router
# [100, 101, 102, 103, ..., 499, 500]  # an apartment complex full of devices and routers
import random

import pride.components.base

def get_nodes_from_groups(groups):
    nodes = []
    for group in groups:
        nodes.extend(group.nodes)    
    return set(nodes)    

class Broadcast_Network_Simulator(pride.components.base.Base):
    
    defaults = {"simulation_time" : 0.00, "time_increment" : 0.005}
    mutable_defaults = {"groups" : list}
    required_attributes = ("groups", )
    
    def run(self):        
        time_increment = self.time_increment
        self.simulation_time += time_increment                        
        print "Current time: ", self.simulation_time        
        for node in get_nodes_from_groups(self.groups):
            node.update_state(time_increment)
      
    @classmethod
    def generate_random_network(cls, node_types, group_types, network_size):
        node_types = [random.choice(node_types).generate_random_node() for count in range(network_size)]
        groups = [random.choice(group_types).generate_random_group(node_types, network_size) for count in range(network_size)]
        return cls(groups=groups)
        
    @classmethod
    def unit_test(cls):           
        node_types = [Broadcast_Node_Simulator]
        group_types = [Node_Group]
        network_size = 2
        simulator = cls.generate_random_network(node_types, group_types, network_size)        
        connecting_node = Broadcast_Node_Simulator(latency=.005, name="router")
        simulator.groups[0].add(connecting_node)
        simulator.groups[1].add(connecting_node)
      #  simulator.groups[2].add(connecting_node)
        print "Name of routing node: {}".format(connecting_node)
        for count in range(16):            
            simulator.run()
        
    
class Packet(object):
        
    header = ("sender_id", "timestamp", "message_counter", "data")
    
    def __init__(self, **kwargs):
        super(Packet, self).__init__()
        for field_name in self.header:
            try:
                value = kwargs.pop(field_name)
            except KeyError:
                raise ValueError("Attempted to create invalid packet; Missing field '{}'".format(field_name))
            setattr(self, field_name, value)
            
    def __str__(self):        
        field_values = ', '.join("{}={}".format(field_name, getattr(self, field_name)) for field_name in self.header)
        return self.__class__.__name__ + '(' + field_values + ')'
        
        
class Broadcast_Node_Simulator(pride.components.base.Base):
    
    packet_type = Packet
    latency_categories = {"fast" : .025, "normal" : .1, "slow" : .75}
    defaults = {"simulation_time" : 0.00, "latency" : 0.100, "_counter" : 0}
    flags = {"name" : None, "_next_send_time" : 0.00}
    mutable_defaults = {"groups" : list}
    verbosity = {"update_state" : "vvv"}
    
    def __init__(self, *args, **kwargs):
        super(Broadcast_Node_Simulator, self).__init__(*args, **kwargs)
        self.name = self.name or id(self)       
        
    def update_state(self, simulation_time):
        self.simulation_time += simulation_time
        #print("{} waiting for {}; current: {}; ready: {}".format(self.name, self._next_send_time, self.simulation_time, self._next_send_time <= self.simulation_time))        
        if self._next_send_time <= self.simulation_time:            
            self._next_send_time = self.simulation_time + self.latency
            packet = self.broadcast('')            
            for node in get_nodes_from_groups(self.groups):
                if node is not self:
                    self.alert("Sending to: {}".format(node), display_name=self, level=self.verbosity["update_state"])
                    node.receive_packet(packet)
    
    def broadcast(self, data):
        self._counter += 1
        return self.packet_type(sender_id=self.name, timestamp=self.simulation_time, 
                                message_counter=self._counter, data=data)        
           
    def receive_packet(self, packet):
        print("{} received: {}".format(self.name, packet))
        
    @classmethod
    def generate_random_node(cls, **kwargs):
        kwargs.setdefault("latency", cls.latency_categories[random.choice(cls.latency_categories.keys())])
        node = cls(**kwargs)
        return node       

                    
class Node_Group(pride.components.base.Base):
                
    mutable_defaults = {"nodes" : list}
    required_attributes = ("nodes", )
                        
    def __init__(self, *args, **kwargs):
        super(Node_Group, self).__init__(*args, **kwargs)
        for node in self.nodes:
            node.groups.append(self)
            super(Node_Group, self).add(node)
            
    def add(self, node):        
        #assert node not in self.nodes
        node.groups.append(self)        
        self.nodes.append(node)
        super(Node_Group, self).add(node)                
        
    def remove(self, node):
        node.groups.remove(self)
        try:
            self.nodes.remove(node)
        except ValueError:
            pass
        super(Node_Group, self).remove_node(node)             
                    
    @classmethod
    def generate_random_group(cls, possible_node_types, group_size):        
        nodes = [random.choice(possible_node_types).generate_random_node() for node_number in range(group_size)]
        return cls(nodes=nodes)        
        
    def delete(self):
        for node in self.nodes:
            self.remove(node)
        super(Node_Group, self).delete()
        
# message delivery options -> specific recipient        
#                          -> broadcast (all receivers)
#                          -> multicast (like broadcasting to individual groups)

# request type options:
# routing only -> data transfer only, no storage required # for send/recv network communication

# network storage -> bulk media storage (push/upload)   # for nodes with LOTS of free space
#                 -> web page storage   (push/upload)   # for commonly accessed web pages, which are only modestly sized
#                 -> temporary data caching (push/upload) # some data has a limited lifespan
#                 -> data retrieval     (pull/download) # for downloading


# ttl 

class Protocol_Packet(Packet):
    
    header = ("sender_id", "timestamp", "counter", "routing_method", "request_type")
    
    
class Protocol_Simulator(Broadcast_Node_Simulator):
      
    ROUTING_METHOD = {"specific recipient" : 0x00, "broadcast" : 0x01,
                      "multicast" : 0x01}
                      
    def broadcast(self, data):
        self._counter += 1
        packet_info = {"sender_id" : self.name, "timestamp" : self.simulation_time,
                       "counter" : self._counter, "routing_method" : 0x01}
    
if __name__ == "__main__":
    Broadcast_Network_Simulator.unit_test()
    