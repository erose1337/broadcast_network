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
import collections

import pride.components.base
import pride.functions.persistence as persistence

NAME_RESOLUTION_REQUEST = 0x00
NAME_RESOLVER = dict()

def get_nodes_from_groups(groups):
    nodes = []
    for group in groups:
        nodes.extend(group.nodes)    
    return set(nodes)    

class Broadcast_Network_Simulator(pride.components.scheduler.Process):
    
    defaults = {"simulation_time" : 0.00, "time_increment" : 0.005, "priority" : .0}
    mutable_defaults = {"groups" : list}
    required_attributes = ("groups", )
    
    def run(self):                
        time_increment = self.time_increment
        self.simulation_time += time_increment                        
        #print "Current time: ", self.simulation_time        
        for node in get_nodes_from_groups(self.groups):
            node.update_state(time_increment)
      
    @classmethod
    def generate_random_network(cls, node_types, group_types, network_size):        
        node_types = [random.choice(node_types) for count in range(network_size)]        
        groups = [random.choice(group_types).generate_random_group(node_types, network_size) for count in range(network_size)]        
        return cls(groups=groups)
        
    @classmethod
    def unit_test(cls):           
        node_types = [Protocol_Simulator]
        group_types = [Node_Group]
        network_size = 3
        simulator = cls.generate_random_network(node_types, group_types, network_size)          
        connecting_node = Protocol_Simulator(latency=.005, name="router")                
        name_resolution_node = Name_Resolution_Service(name="Name Resolution Service")
        simulator.groups[0].add(connecting_node)        
                        
        for group in simulator.groups:
            for node in group.nodes:
                node.resolve_name(node.nrs_public_key, ("Service0", "Service1"))                                      
                
        simulator.groups[1].add(connecting_node)        
        simulator.groups[2].add(connecting_node)        
        simulator.groups[-1].add(name_resolution_node)        
        print "Name of routing node: {}".format(connecting_node)
        print "Name of NRS node    : {}".format(name_resolution_node)
        print "Complete network: {}".format([[node.reference for node in group.nodes] for group in simulator.groups])
        return simulator
                
    
class Packet(object):
        
    header = ("sender_id", "timestamp", "message_counter", "time_to_live", "data")
    
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
        
    def copy(self):
        attributes = dict((field_name, getattr(self, field_name)) for field_name in self.header)
        return type(self)(**attributes)
        
        
class Broadcast_Node_Simulator(pride.components.base.Base):
    
    packet_type = Packet
        
    latency_categories = {"fast" : .025, "normal" : .1, "slow" : .75}
    defaults = {"simulation_time" : 0.00, "latency" : 0.100}
    flags = {"name" : None, "_next_send_time" : 0.00}
    mutable_defaults = {"groups" : list, "outgoing_packets" : list}
    verbosity = {"update_state" : "vvv"}    
    
    def __init__(self, *args, **kwargs):
        super(Broadcast_Node_Simulator, self).__init__(*args, **kwargs)
        self.name = self.name or id(self)       
        
    def update_state(self, simulation_time):
        self.simulation_time += simulation_time
        #print("{} waiting for {}; current: {}; ready: {}".format(self.name, self._next_send_time, self.simulation_time, self._next_send_time <= self.simulation_time))        
        if self._next_send_time <= self.simulation_time:            
            self._next_send_time = self.simulation_time + self.latency
            for packet in self.outgoing_packets:
                assert isinstance(packet, Packet)
                self._broadcast(packet)
            del self.outgoing_packets[:]
            
    def create_packet(self, **packet_kwargs):
        assert not isinstance(data, Packet)                        
        return self.packet_type(**packet_kwargs)  
        
    def broadcast(self, packet):
        assert isinstance(packet, Packet)
        self.outgoing_packets.append(packet)                
        
    def _broadcast(self, packet):      
        assert isinstance(packet, Packet)
        for node in get_nodes_from_groups(self.groups):
            if node is not self:
                self.alert("Sending {} to: {}".format(hash(packet), node), display_name=self.reference, level=0)#self.verbosity["update_state"])                        
                node.receive_packet(packet.copy())                  
        
    def receive_packet(self, packet):                
        packet.time_to_live -= 1
        assert packet.time_to_live >= 0
        #if packet.recipient == self.name:
        self.handle_received_packet(packet)            
        if packet.time_to_live > 0:
            self.broadcast(packet)
            
    def handle_received_packet(self, packet):
        self.alert("received: {}".format(packet))    
        
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

# "connectivity" layer/protocol:    ttl || packet_id || data
#   - makes data available to network
# "data transfer" layer/protocol:   type0 || hash(public_key) || encrypt(data, public_key)
#   - sends data to specified recipient
#   ! needs a name resolution service
#       - a place that stores name:public_key pairs
#       - network storage with hash(data):data is unforgeable
# data storage layer/protocol:      type1 || key:hash(data) || value:data
#   - stores data in network for some lifetime
# max packet size: 2304 bytes;    256 bytes of header + 2048 bytes of data
# max packet size: 1500 bytes;    256 bytes of header + 1244 bytes of data

# connectivity header
# packet_id:   32
# time_to_live: 1

# transfer header: 1
# hash(public key): 32





class Protocol_Packet(Packet):
    
    header = ("time_to_live", "packet_id", "recipient_id", "data")
    
            
class Name_Resolution_Request(Protocol_Packet): pass
            
class Name_Resolution_Response(Protocol_Packet): pass
       
    
class Protocol_Simulator(Broadcast_Node_Simulator):
      
    packet_type = Protocol_Packet
    name_resolution_request_type = Name_Resolution_Request
    defaults = {"default_time_to_live" : 255, "nrs_public_key" : None, "public_key" : None}       
    mutable_defaults = {"recently_sent_packets" : lambda: collections.deque(maxlen=65536),
                        "response_identifiers" : list}
    
    #def __init__(self, *args, **kwargs):
    #    super(Protocol_Simulator, self).__init__(*args, **kwargs)
        #requester_public_key, receiever_id, host_names
        #receiver_id = self.generate_random_number()
        #self.response_identifiers.append(receiver_id)
        #request = (None, receiver_id, ("Service0", "Service1"))
        #packet_data = self.save_data(request)
        #print packet_data
        #assert self.load_data(packet_data) == request
        #packet_id = self.generate_random_number()
        #self.outgoing_packets.append(Name_Resolution_Request(time_to_live=self.time_to_live, 
        #                                                     data=packet_data,
        #                                                     packet_id=packet_id))
        #self.resolve_name(self.nrs_public_key, ("Service0", "Service1"))
        
    def broadcast(self, packet):                
        if packet.packet_id not in self.recently_sent_packets:
            self.outgoing_packets.append(packet)
            self.recently_sent_packets.append(packet.packet_id)

    def create_packet(self, packet_type, **packet_kwargs):
        assert not isinstance(packet_kwargs["data"], Packet)               
        packet_kwargs.setdefault("time_to_live", self.default_time_to_live)
        packet_kwargs.setdefault("packet_id", self.generate_random_number())
        return packet_type(**packet_kwargs)  
        
    def generate_random_number(self, _size=(2 ** 257) - 1):
        return random.randint(0, _size)
                   
    def resolve_name(self, nrs_public_key, host_names):
        self.alert("Issuing name resolution request")
        return_address = self.reference#self.generate_random_number()                
        request = self.save_data((self.public_key, return_address, host_names))
        nrs_id = self.hash_public_key(nrs_public_key)
        
        packet = self.create_packet(self.name_resolution_request_type, 
                                    data=self.encrypt(request, nrs_public_key),
                                    recipient_id=nrs_id,
                                    request_type=NAME_RESOLUTION_REQUEST)
        self.broadcast(packet)        
        self.response_identifiers.append(return_address)        
        self._timeout_instruction = pride.Instruction(self.reference, "handle_timeout", packet)
        self._timeout_instruction.execute(priority=1)
                
    def handle_timeout(self, packet):
        self.alert("Request {} timed out".format(packet.packet_id))
        raise SystemExit()
        
    def handle_received_packet(self, packet):          
        if hasattr(packet, "recipient_id"):
           # self.alert("Obtained NRS response packet; determining recipient...")
            if packet.recipient_id in self.response_identifiers:
                self.handle_name_resolution_response(packet)                
                self._timeout_instruction.unschedule()
                
    def handle_name_resolution_response(self, packet):
        super(Protocol_Simulator, self).handle_received_packet(packet)        
        self.response_identifiers.remove(packet.recipient_id)
        resolved_names = self.load_data(self.decrypt(packet.data))
        for name, public_key in resolved_names:            
            NAME_RESOLVER[name] = public_key            
        
    def save_data(self, data):
        return persistence.save_data(data)
        
    def load_data(self, data):
        return persistence.load_data(data)
        
    def decrypt(self, data):
        #cryptoprovider.asymmetric.encrypt(data, self.public_key)
      #  self.alert("Encryption not enabled, returning plaintext packet data", level=0)
        return data
        #raise NotImplementedError()
    
    def encrypt(self, data, public_key):
     #   self.alert("Encryption not enabled, returning plaintext packet data", level=0)
        return data
        
    def hash_public_key(self, public_key):
        #self.alert("cryptography not installed, using insecure hash", level=0)
        return hash(public_key)
    
    
class Name_Resolution_Service(Protocol_Simulator):
        
    packet_type = Name_Resolution_Response
    
    defaults = {"public_key" : None, "private_key" : None}
    mutable_defaults = {"host_names" : lambda: {"Service0" : "Service0-PublicKey", "Service1" : "Service1-PublicKey"}}
    
    def handle_received_packet(self, packet):
        if getattr(packet, "recipient_id", None) == self.hash_public_key(self.public_key):            
            data = self.decrypt(packet.data) # the ability to decrypt proves the authenticity of the response
            requester_public_key, receiver_id, host_names = self.load_data(data)
            #host_names = self.load_data(packet.data)
            resolved_hosts = tuple((host_name, self.host_lookup(host_name)) for host_name in host_names)        
            response = self.encrypt(self.save_data(resolved_hosts), requester_public_key)
            self.broadcast(self.create_packet(self.packet_type, data=response, recipient_id=receiver_id))
            print "Broadcasting host info for: ", receiver_id
            #raw_input()
            
    def host_lookup(self, host_name):
        return self.host_names[host_name]
        
if __name__ == "__main__":
    Broadcast_Network_Simulator.unit_test()
    