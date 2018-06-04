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
NAME_RESOLUTION_RESPONSE = 0X01
NAME_RESOLVER = dict()

ROOT_NAME_RESOLUTION_PUBLIC_KEY = hash(0xc0ffee)

MAX_PACKET_ID_SIZE = (2 ** 257) - 1
MAX_SEQUENCE_SIZE = (2 ** 32) - 1
MAX_DATA_SIZE = 2048

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
        node_types = [Connectivity_Layer]
        group_types = [Node_Group]
        network_size = 5
        simulator = cls.generate_random_network(node_types, group_types, network_size)          
        connecting_node = Connectivity_Layer(latency=.005, name="router")                
        name_resolution_node = Name_Resolution_Node(name="Name Resolution Service", public_key=ROOT_NAME_RESOLUTION_PUBLIC_KEY)
        name_request_node = Name_Resolution_Node(name="Name Resolution Requester")
        #simulator.groups[0].add(connecting_node)        
        
        for group in simulator.groups:
            #for node in group.nodes:
            #    node.resolve_name(node.nrs_public_key, ("Service0", "Service1"))                                      
            group.add(connecting_node)
        #simulator.groups[1].add(connecting_node)        
        #simulator.groups[2].add(connecting_node)        
        simulator.groups[-1].add(name_resolution_node) 
        simulator.groups[0].add(name_request_node)
        name_request_node.resolve_name(name_request_node.nrs_public_key, ("Service0", "Service1"))        
        print "Name of routing node: {}".format(connecting_node)
        print "Name of NRS node    : {}".format(name_resolution_node)
        print "Complete network: {}".format([[node.reference for node in group.nodes] for group in simulator.groups])
        return simulator
                
    
class Packet(object):
        
    header = ("sender_id", "timestamp", "message_counter", "time_to_live", "data")    
        
    def _get_data(self):
        return self._data
    def _set_data(self, value):
        if len(value) > self.max_data_size:
            raise ValueError("Data too large to fit into packet")
        else:
            self._data = value
    data = property(_get_data, _set_data)
    
    def __init__(self, **kwargs):
        super(Packet, self).__init__()
        self._data = ''
        self.max_data_size = MAX_DATA_SIZE
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
        
class Protocol_Packet(Packet):
    
    header = ("time_to_live", "packet_id", "recipient_id", "data")
    
            
class Name_Resolution_Request(Protocol_Packet): pass
            
class Name_Resolution_Response(Protocol_Packet): pass

        
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

        
class Node_Simulator(pride.components.base.Base):      
        
    latency_categories = {"fast" : .025, "normal" : .1, "slow" : .75}
    defaults = {"simulation_time" : 0.00, "latency" : 0.100,
                "min_packet_loss" : 0, "max_packet_loss" : 100, "packet_loss_threshold" : 50}
    flags = {"name" : None, "_next_send_time" : 0.00}
    mutable_defaults = {"groups" : list, "outgoing_packets" : list}
    verbosity = {"update_state" : "vvv"}    
    
    def __init__(self, *args, **kwargs):
        super(Node_Simulator, self).__init__(*args, **kwargs)
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
            
    def create_packet(self, packet_type, **packet_kwargs):
        assert not isinstance(data, Packet)                        
        return packet_type(**packet_kwargs)  
        
    def broadcast(self, packet):     
        assert isinstance(packet, Packet)
        if packet.packet_id not in self.recently_sent_packets:
            self.outgoing_packets.append(packet)
            self.recently_sent_packets.append(packet.packet_id)
            
    def _broadcast(self, packet):      
        assert isinstance(packet, Packet)
        for node in get_nodes_from_groups(self.groups):
            if node is not self:
                #self.alert("Sending {} to: {}".format(hash(packet), node), display_name=self.reference, level=0)#self.verbosity["update_state"])                        
                if random.randint(self.min_packet_loss, self.max_packet_loss) > self.packet_loss_threshold:                
                    node.receive_packet(packet.copy())                  
                              
    def receive_packet(self, packet):                
        raise NotImplementedError()
                    
    def handle_received_packet(self, packet):
        self.alert("received: {}".format(packet))    
        
    @classmethod
    def generate_random_node(cls, **kwargs):
        kwargs.setdefault("latency", cls.latency_categories[random.choice(cls.latency_categories.keys())])
        node = cls(**kwargs)
        return node       
                  

class Connectivity_Layer(Node_Simulator):

    defaults = {"default_time_to_live" : 255, "max_packet_id" : MAX_PACKET_ID_SIZE,
                "public_key" : None, "private_key" : None}
    mutable_defaults = {"recently_sent_packets" : lambda: collections.deque(maxlen=65536),
                        "response_identifiers" : list, "already_handled_packets" : lambda: collections.deque(maxlen=65536)}
    
    def create_packet(self, packet_type, **packet_kwargs):
        assert not isinstance(packet_kwargs["data"], Packet)               
        packet_kwargs.setdefault("time_to_live", self.default_time_to_live)
        packet_kwargs.setdefault("packet_id", random.randint(0, self.max_packet_id))
        return packet_type(**packet_kwargs) 
        
    def receive_packet(self, packet):
        packet.time_to_live -= 1
        assert packet.time_to_live >= 0        
        if packet.time_to_live > 0:
            self.broadcast(packet)
        #self.alert("Checking packet {} {}".format(packet.recipient_id, self.response_identifiers))               
        if packet.packet_id not in self.already_handled_packets:
            if packet.recipient_id in self.response_identifiers:
                self.response_identifiers.remove(packet.recipient_id)
                self.handle_received_packet(packet)                     
            elif packet.recipient_id == self.hash_public_key(self.public_key): # change to constant time comparison
                self.handle_received_packet(packet)        
            self.already_handled_packets.append(packet.packet_id)
        
    def hash_public_key(self, public_key):
        #self.alert("cryptography not installed, using insecure hash", level=0)
        return hash(public_key)
        
        
class Reliable_Packet(Protocol_Packet):
            
    header = ("time_to_live", "packet_id", "recipient_id", "packet_number", "packet_total", "datagram_number", "data")
    
    
class Reliable_Connectivity_Layer(Connectivity_Layer):    
    # delivers reliable, ordered datagrams 
    defaults = {"default_sequence_number" : 0, "max_sequence_size" : MAX_SEQUENCE_SIZE,
                "packet_number" : 0, "datagram_number" : 0}
    mutable_defaults = {"outgoing_datagrams" : dict}
    
    def send_to(self, data, receiver):
        max_data_size = Reliable_Packet.max_data_size
        number_of_packets, extra = divmod(data, max_data_size)
        if extra:
            number_of_packets += 1
        
        series = []        
        
        datagram_number = self.datagram_number
        for packet_number in range(number_of_packets):                
            packet = self.create_packet(Reliable_Packet, packet_number=packet_number,
                                                         packet_total=number_of_packets,
                                                         datagram_number=datagram_number,
                                                         data=data[(packet_number * max_data_size):((packet_number + 1) * max_data_size)])
            series.append(packet)
            self.broadcast(packet)
        self.outgoing_datagrams[packet_id] = series          
    
    def handle_received_packet(self, packet):
        try:
            datagram_pieces = self.incoming_requests[packet.packet_id]
        except KeyError:
            datagram_pieces = self.incoming_requests[packet.packet_id] = set((packet, ))
        else:                        
            datagram_pieces.add(packet)
        
        if packet.packet_total == len(datagram_pieces): # all pieces obtained, re-assemble            
            data = ''.join((item.data for item in sorted(datagram_pieces, key=operator.attrgetter("packet_number"))))
            del self.incoming_requests[packet.packet_id]                        
            self.receive_from(data, packet.recipient_id)
        else:
            try:
                nack_instruction = self.nack_timer[packet.packet_id]
            except KeyError:
                nack_instruction = pride.Instruction(self.reference, "nack", packet.packet_id)
                self.nack_timer[packet.packet_id] = nack_instruction
            else:
                nack_instruction.unschedule()
            nack_instruction.execute(priority=self.nack_delay)
            
    def nack(self, packet):
        datagram_pieces = set((_packet.packet_number for _packet in self.incoming_requests[packet.packet_id]))
        all_pieces = set(range(packet.packet_total))
        nack_numbers = all_pieces.difference(datagram_pieces)
        nack_request = self.create_packet(Nack_Packet, 
        
        
    def receive_from(self, data, return_address):
        self.alert("Received {} from {}".format(data, return_address)
        
        
# message delivery options -> specific recipient        
#                          -> broadcast (all receivers)
#                          -> multicast (like broadcasting to individual groups)

# request type options:
# routing only -> data transfer only, no storage required # for send/recv network communication

# network storage -> bulk media storage (push/upload)   # for nodes with LOTS of free space
#                 -> web page storage   (push/upload)   # for commonly accessed web pages, which are only modestly sized
#                 -> temporary data caching (push/upload) # some data has a limited lifespan
#                 -> data retrieval     (pull/download) # for downloading

# "connectivity" layer/protocol:    ttl || packet_id || recipient_id || data
#   - makes data available to network
# "data transfer" layer/protocol:   ttl || recipient_id || packet_id || sequence_number || datagram_number
#   - sends ordered, reliable datagrams to specified recipient
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
       
       
class Name_Resolution_Node(Connectivity_Layer):
              
    defaults = {"nrs_public_key" : ROOT_NAME_RESOLUTION_PUBLIC_KEY, "timeout_interval" : 2}       
    mutable_defaults = {"host_names" : lambda: {"Service0" : "Service0-PublicKey", 
                                                "Service1" : "Service1-PublicKey"}}                              
                   
    def resolve_name(self, nrs_public_key, host_names):
        self.alert("Issuing name resolution request")
        return_address = random.randint(0, MAX_PACKET_ID_SIZE)
        request = (NAME_RESOLUTION_REQUEST, (self.public_key, return_address, host_names))
        packaged_data = self.package(request, nrs_public_key)
        nrs_id = self.hash_public_key(nrs_public_key)
        packet = self.create_packet(Name_Resolution_Request,
                                    data=packaged_data,
                                    recipient_id=nrs_id)
        self.broadcast(packet)        
        self.response_identifiers.append(return_address)                
        self._timeout_instruction = pride.Instruction(self.reference, "handle_timeout", packet)
        self._timeout_instruction.execute(priority=self.timeout_interval)
                
    def handle_timeout(self, packet):
        self.alert("Name Resolution Request timed out; Re-issuing".format(hash(packet)))
        new_packet = packet.copy()
        packet.packet_id = random.randint(0, self.max_packet_id)
        self.broadcast(packet)
        self._timeout_instruction = pride.Instruction(self.reference, "handle_timeout", packet)
        self._timeout_instruction.execute(priority=self.timeout_interval)
        #raise SystemExit()
        
    def handle_received_packet(self, packet):                                  
        # decrypt packet data to learn the request type and what to do with the data
        request_type, data = self.unpackage(packet.data)
        if request_type == NAME_RESOLUTION_RESPONSE:
            self.handle_name_resolution_response(packet)                
            self._timeout_instruction.unschedule()
        elif request_type == NAME_RESOLUTION_REQUEST:
            self.handle_name_resolution_request(packet)
        else:
            self.alert("Received unknown packet type: {}".format(request_type), level=0)
                
    def handle_name_resolution_response(self, packet):
        super(Name_Resolution_Node, self).handle_received_packet(packet)                
        request_type, resolved_names = self.unpackage(packet.data)
        for name, public_key in resolved_names:            
            NAME_RESOLVER[name] = public_key            
        
    def package(self, data, public_key):
        return self.encrypt(self.save_data(data), public_key)
        
    def unpackage(self, data):
        return self.load_data(self.decrypt(data))
        
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
            
    def handle_name_resolution_request(self, packet):
        self.alert("Handling Name Resolution Request")
        request_type, request_data = self.unpackage(packet.data) # the ability to decrypt proves the authenticity of the response
        requester_public_key, receiver_id, host_names = request_data        
        resolved_hosts = tuple((host_name, self.host_lookup(host_name)) for host_name in host_names)
        response = self.package((NAME_RESOLUTION_RESPONSE, resolved_hosts), requester_public_key)        
        self.broadcast(self.create_packet(Name_Resolution_Response, data=response, recipient_id=receiver_id))
        print "Broadcasting host info for: ", receiver_id

    def host_lookup(self, host_name):
        return self.host_names[host_name]        
        
if __name__ == "__main__":
    Broadcast_Network_Simulator.unit_test()
    