# broadcast network simulator
# each frame, advance simulation by x ms
# each node sends every y ms (with some variance)

class Broadcast_Network_Simulator(object):
    
    def __init__(self, nodes, simulation_time=0.00, time_increment=.005):
        self.nodes = nodes
        self.simulation_time = simulation_time
        self.time_increment = time_increment
        
    def run(self):
        time_increment = self.time_increment
        self.simulation_time += time_increment                
        packets = []
        print "Current time: ", self.simulation_time
        for node_number, node in enumerate(self.nodes):
            packet = node.update_state(time_increment)            
            if packet is not None:
                packets.append((node_number, packet))
        for senders_number, packet in packets:
            for node_number, node in enumerate(self.nodes):
                if node_number != senders_number:
                    node.receive_packet(packet)
                    
    @classmethod
    def unit_test(cls):
        fast_node = Broadcast_Node_Simulator(latency=0.025, name="fast node")
        normal_node = Broadcast_Node_Simulator(latency=0.100, name="normal node")
        slow_node = Broadcast_Node_Simulator(latency=0.750, name="slow node")
        nodes = [fast_node, normal_node, slow_node]
        simulator = cls(nodes)
        for count in range(760 / 5):
            simulator.run()
        
    
class Packet(object):
        
    header = ("sender_id", "timestamp", "message_counter")
    
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
        
        
class Broadcast_Node_Simulator(object):
    
    def __init__(self, simulation_time=0.00, latency=0.100, _counter=0, name=None):
        super(Broadcast_Node_Simulator, self).__init__()
        self.simulation_time = simulation_time
        self.latency = latency
        self._next_send_time = simulation_time
        self._counter = _counter
        self.name = name or id(self)
        
    def update_state(self, simulation_time):
        self.simulation_time += simulation_time
        #print("{} waiting for {}; current: {}; ready: {}".format(self.name, self._next_send_time, self.simulation_time, self._next_send_time <= self.simulation_time))        
        if self._next_send_time <= self.simulation_time:            
            self._next_send_time = self.simulation_time + self.latency
            return self.generate_packet()
    
    def generate_packet(self):
        self._counter += 1
        return Packet(sender_id=self.name, timestamp=self.simulation_time, message_counter=self._counter)        
       
    def receive_packet(self, packet):
        print("{} received: {}".format(self.name, packet))
        
        
if __name__ == "__main__":
    Broadcast_Network_Simulator.unit_test()
    