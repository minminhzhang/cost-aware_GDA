from pulp import *
import collections
import threading
import json
import math
import time
import copy

sys.path.append('./gen-py')

from ReduceTaskPlacementIface import *

from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer

class ReduceTaskPlacement_handler:
	def __init__(self, placement):
		self.placement = placement

	def getCostBoundary(self, inputs):
		return None

	def evaluate(self, inputs):	
		if inputs == None:
			inputs = self.placement.load_test_inputs()

		_inputs = json.loads(inputs)


		with open('inputs', 'w') as outfile:
		  json.dump(_inputs, outfile)

		# check current stage
		self.goals_init = _inputs['goals']

		if 'stage' in self.goals_init and self.goals_init['stage'] == 'shuffle':
			return self.placement.evaluate_shuffle(_inputs)
		
		return self.placement.evaluate_map(_inputs)

		
class TripS:
	msg = 0
	epgap = 0.05

	use_cplex = False

	def run_server(self, server_port):
        # set handler to our implementation
		handler = ReduceTaskPlacement_handler(self)

		processor = ReduceTaskPlacementIface.Processor(handler)
		transport = TSocket.TServerSocket(port=server_port)
		tfactory = TTransport.TFramedTransportFactory()
		pfactory = TBinaryProtocol.TBinaryProtocolFactory()

		# set server
		server = TServer.TThreadedServer(processor, transport, tfactory, pfactory, daemon=True)

		print('[Reduce Task Placement] Starting applications server port:' + str(server_port))
		server.serve()

	def str_db(self, value, _round):

		return str(round(value, _round))
		
	def getBytesFromMB(self, MB):

		_bytes = MB 
		return _bytes

	def getCostFromGBBased(self, GB):

		_cost = GB 
		return _cost
	
# map stage variable by fractions  MZ

	def init_map_variable(self):
		minmax_latency_map = LpVariable('min max latency map', 0)

		# track fractions
		task_placement_fraction_map = LpVariable.dicts("task_placement_fraction_map", [(_from, _to) for _from in self.dc_list for _to in self.dc_list], 0, 1, cat='Continuous')
	
		return minmax_latency_map, task_placement_fraction_map

# map task constraints by fractions  MZ
	def set_constraints_map(self, prob, minmax_latency_map, task_placement_fraction_map, min_cost, cost_for_min_latency, latency_for_min_cost, min_latency):

		init_data_fraction_each_dc_map = {}
		for _dc in self.dc_list:
			init_data_fraction_each_dc_map[_dc] = self.data_size_each_dc_map[_dc] / float(self.total_data_size_map)

		prob += lpSum(task_placement_fraction_map[(_from, _to)] for _from in self.dc_list for _to in self.dc_list) < 1.0

		for _from in self.dc_list:
			prob += (init_data_fraction_each_dc_map[_from] >= lpSum(task_placement_fraction_map[(_from, _to)] for _to in self.dc_list))	

		desired_cost_map = min_cost + (cost_for_min_latency - min_cost) * self.goals['desired_cost_spot']
		allowed_max_cost_map = min_cost * self.goals['allowed_cost_increasing_rate']

		cost_constraint = desired_cost_map
		if cost_constraint <= 0:
			cost_constaint = min_cost
		cost_constaint_without_round = self.min_cost_without_round + (self.min_latency_cost_without_round - self.min_cost_without_round) * self.goals['desired_cost_spot']

		desired_latency_map = min(latency_for_min_cost - ((latency_for_min_cost - min_latency) * self.goals['desired_latency_spot']), latency_for_min_cost - latency_for_min_cost * self.goals['allowed_latency_decreasing_rate'])
		if desired_latency_map < min_latency:
			desired_latency_map = min_latency
		
		print('Desired cost_map: ' + self.str_db(cost_constraint, 8) + '\n')

		data_transfer_cost_map = lpSum(task_placement_fraction_map[(_from, _to)] \
									* self.total_data_size_map \
									* self.cost[_from]['network_cost'][_to] \
									for _from in self.dc_list for _to in self.dc_list)
		# total_cost = 0
		for _from in self.dc_list:
			data_size_each_dc_transferred = self.data_size_each_dc_map[_from] \
												- lpSum(task_placement_fraction_map[(_from, _to)] \
														* self.total_data_size_map \
														for _to in self.dc_list) \
												+ lpSum(task_placement_fraction_map[(_f, _from)] \
														* self.total_data_size_map \
														for _f in self.dc_list)
			compute_time_map = data_size_each_dc_transferred \
										/ self.monitoring_info[_from]['compute_resources']['computing_bandwidth'] \
										/ self.monitoring_info[_from]['compute_resources']['node_cores']

			for _to in self.dc_list:
				data_transfer_time_map = task_placement_fraction_map[(_from, _to)] \
							* self.total_data_size_map \
							/ self.monitoring_info[_from]['network_bandwidth'][_to]

				prob += minmax_latency_map >= data_transfer_time_map + compute_time_map	
				compute_cost_map = lpSum((data_transfer_time_map + compute_time_map) \
								* self.monitoring_info[_dc]['compute_resources']['node_cores'] \
								* self.cost[_dc]['compute_cost'] \
								for _dc in self.dc_list)
				prob += cost_constaint_without_round >= data_transfer_cost_map + compute_cost_map
		
		prob += minmax_latency_map

#map stage object
	def set_objective_map(self, prob, minmax_latency_map, task_placement_fraction_map):
		prob += minmax_latency_map, 'min_latency_with_cost_constraint'

#shuffle stage variable
	def init_variable(self):

		max_latency_variable = LpVariable('min largest latency', 0)

		task_placement_variable = LpVariable.dicts('task_placement', [(str(reduce_task_id), _to) for reduce_task_id in range(0, self.number_of_reducer) for _to in self.dc_list], 0, 1, LpBinary)

		return max_latency_variable, task_placement_variable

#shuffle stage constraint
	def set_constraints(self, prob, max_latency_variable, task_placement_variable, min_cost, cost_for_min_latency, latency_for_min_cost, min_latency):
		#constraints
		for reduce_task_id in range(0, self.number_of_reducer):
			prob += lpSum(task_placement_variable[(str(reduce_task_id), _to)] for _to in self.dc_list) == 1.0, 'For each partition (reducer id: ' + str(reduce_task_id) + ')'

		###############################################
		#find desired cost range
		desired_cost = min_cost + (cost_for_min_latency - min_cost) * self.goals['desired_cost_spot']
		allowed_max_cost = min_cost * self.goals['allowed_cost_increasing_rate']
		cost_constraint = min(desired_cost, allowed_max_cost)

		if cost_constraint <= 0:
			cost_constraint = min_cost

		##############################################
		#Set desired latency
		desired_latency = min(latency_for_min_cost - ((latency_for_min_cost - min_latency) * self.goals['desired_latency_spot']), latency_for_min_cost - latency_for_min_cost * self.goals['allowed_latency_decreasing_rate'])

		if desired_latency < min_latency:
			desired_latency = min_latency
		
		print('Desired cost: ' + self.str_db(cost_constraint, 8) + ', latency: ' + self.str_db(desired_latency, 8) + '\n')

		data_transfer_cost_shuffle = lpSum(task_placement_variable[(str(reduce_task_id), _to)] \
										* self.data_size[_from][str(reduce_task_id)] \
										* self.cost[_from]['network_cost'][_to] \
										for reduce_task_id in range(0, self.number_of_reducer) for _from in self.dc_list for _to in self.dc_list)

		for _from in self.dc_list:
			for _to in self.dc_list:
				data_transfer_time_shuffle = lpSum(task_placement_variable[(str(reduce_task_id), _to)] \
													* self.data_size[_from][str(reduce_task_id)] \
													/ self.monitoring_info[_from]['network_bandwidth'][_to] \
													for reduce_task_id in range(0, self.number_of_reducer))

				data_size_each_dc_transferred = lpSum(task_placement_variable[(str(reduce_task_id), _from)] \
													* self.data_size[_f][str(reduce_task_id)] \
														for reduce_task_id in range(0, self.number_of_reducer) for _f in self.dc_list)
				
				compute_time_shuffle = data_size_each_dc_transferred \
											/ self.monitoring_info[_from]['compute_resources']['computing_bandwidth'] \
											/ self.monitoring_info[_from]['compute_resources']['node_cores']
				

				compute_cost_shuffle = lpSum((data_transfer_time_shuffle + compute_time_shuffle) \
								* self.monitoring_info[_from]['compute_resources']['node_cores'] \
								* self.cost[_from]['compute_cost'] \
								for _from in self.dc_list)
				prob += cost_constraint >= data_transfer_cost_shuffle + compute_cost_shuffle
				prob += max_latency_variable >= data_transfer_time_shuffle + compute_time_shuffle

		print('cost_constraint ' + str(cost_constraint))

#shuffle stage 
	def set_objective(self, prob, max_latency_variable, task_placement_variable):
		prob += max_latency_variable

#shuffle stage
	def get_cost_boundary(self, inputs):
		self.set_inputs(inputs)

		r = self.get_r_min_cost()

		min_cost_c_network, min_cost_c_compute = self.get_cost_with_r(r)
		min_cost_l_network, min_cost_l_computation = self.get_latency_with_r(r)
		min_cost = min_cost_c_network + min_cost_c_compute
		min_cost_latency = min_cost_l_network + min_cost_l_computation
 
		min_latency_r = self.get_r_min_latency()

		min_latency_c_network, min_latency_c_compute = self.get_cost_with_r(min_latency_r)
		min_latency_network, min_latency_computation = self.get_latency_with_r(min_latency_r)
		cost_for_min_latency = min_latency_c_network + min_latency_c_compute
		min_latency = min_latency_network + min_latency_computation
		return min_cost, min_cost_latency, cost_for_min_latency, min_latency

# map stage 
	def get_cost_boundary_map(self, inputs):
		self.set_inputs_map(inputs)

		fraction_min_cost = self.get_f_min_cost_map()

		min_cost, network_cost_min_cost, compute_cost_min_cost = self.get_cost_with_f(fraction_min_cost)
		min_cost_without_round = self.get_cost_with_f_without_round(fraction_min_cost)
		min_cost_latency, min_cost_network_latency, min_cost_compute_latency = self.get_latency_with_f(fraction_min_cost)
		min_cost_latency_without_round = self.get_latency_with_f_without_round(fraction_min_cost)

		fraction_min_latency = self.get_f_min_latency_map()

		min_latency_cost_map, network_cost_min_latency, compute_cost_min_latency = self.get_cost_with_f(fraction_min_latency)
		min_latency_cost_without_round = self.get_cost_with_f_without_round(fraction_min_latency)
		min_latency, min_latency_network, min_latency_compute = self.get_latency_with_f(fraction_min_latency)
		min_latency_without_round = self.get_latency_with_f_without_round(fraction_min_latency)

		return min_cost, min_cost_without_round, min_latency_cost_map, min_latency_cost_without_round, min_cost_latency, min_cost_latency_without_round, min_latency, min_latency_without_round

# map stage evaluation  MZ
	def evaluate_map(self, inputs):

		start_map = time.time()
		self.set_inputs_map(inputs)
		min_cost_start_map = time.time()

		f = self.get_f_min_cost_map()
		total_min_cost_round, network_min_cost_round, compute_min_cost_round = self.get_cost_with_f(f)
		self.min_cost_without_round = self.get_cost_with_f_without_round(f)
		total_min_cost_latency_round, min_cost_network_latency, min_cost_compute_latency = self.get_latency_with_f(f)
		self.total_min_cost_latency_without_round = self.get_latency_with_f_without_round(f)

		min_cost_elapse_time = time.time() - min_cost_start_map

		print("Min cost map: " + str(total_min_cost_round) + " latency map: " + self.str_db(total_min_cost_latency_round, 12)) \

		min_latency_start_map = time.time()

		min_latency_f = self.get_f_min_latency_map()
		total_cost_for_min_latency, network_cost_min_latency, compute_cost_min_latency  = self.get_cost_with_f(min_latency_f)
		self.min_latency_cost_without_round = self.get_cost_with_f_without_round(min_latency_f)
		total_latency_min_round, network_latency_min, compute_latency_min = self.get_latency_with_f(min_latency_f)
		self.latency_min_without_round = self.get_latency_with_f_without_round(f)

		min_latency_elapse_time = time.time() - min_latency_start_map

		print("Min Latency cost map: " + str(total_cost_for_min_latency) + " latency map: " + self.str_db((total_latency_min_round), 12)) 

		minmax_latency_map, task_placement_fraction_map = self.init_map_variable()

		prob = LpProblem('Min map latency', LpMinimize)

		self.set_objective_map(prob, minmax_latency_map, task_placement_fraction_map)
		#set constraints
		self.set_constraints_map(prob, minmax_latency_map, task_placement_fraction_map, total_min_cost_round, total_cost_for_min_latency, total_min_cost_latency_round, total_latency_min_round)
		
		prob.writeLP('model_map')

		if self.use_cplex == True:
			solver = CPLEX(msg=self.msg, epgap=self.epgap)
		else:
			solver = GUROBI(msg=self.msg, epgap=self.epgap)
		status = prob.solve(solver)

		prob.writeLP('trips_map.lp')

		end = time.time()

		if status > 0:

			fraction_float = self.get_f_with_variable_map(task_placement_fraction_map)

			map_task_placement = self.get_map_task_placement(fraction_float)
			with_estimated_latency = self.estimate_latency(map_task_placement)
			self.print_result_map(map_task_placement, fraction_float, 'Cost Spot: ' + str(self.goals['desired_cost_spot']) + ' Performance Spot: ' + str(self.goals['desired_latency_spot']))
			print('Take time: ' + str(end - start_map) + ' ms, min cost: ' + str(min_cost_elapse_time) + ' ms, min latency: ' + str(min_latency_elapse_time) + ' ms' + '\n')

			return json.dumps(with_estimated_latency)		
		else:
			print('failed to determine a placement')
			return False

# shuffle stage
	def evaluate_shuffle(self, inputs):
		start = time.time()
		self.set_inputs(inputs)

		#only for minimize data tranfer
		if self.goals['min_network_usage'] == True:
			reduce_task_placement = self.get_min_network_usage_with_inputs()
			self.print_result(reduce_task_placement, 'Min Network Usage')
			end = time.time()
			print('Min Network - Take time: ' + str(end - start) + ' ms')

                        with_estimated_latency = self.estimate_latency_shuffle(reduce_task_placement)
                        return json.dumps(with_estimated_latency)

		if 'min_network_latency' in self.goals and self.goals['min_network_latency'] == True:
			reduce_task_placement = self.get_min_latency_with_inputs()
			self.print_result(reduce_task_placement, 'Min Network Latency')
			end = time.time()
			print('Min Latency - Take time: ' + str(end - start) + ' ms')
		
               
                        with_estimated_latency = self.estimate_latency_shuffle(reduce_task_placement)
                        return json.dumps(with_estimated_latency)
                
		min_cost_start = time.time()

		r = self.get_r_min_cost()
		min_cost_c_network, min_cost_c_compute = self.get_cost_with_r(r)
		min_cost = min_cost_c_network + min_cost_c_compute

		network_latency, compute_latency = self.get_latency_with_r(r)
		latency_for_min_cost = network_latency + compute_latency #[0] + latency[1]

		min_cost_elapse_time = time.time() - min_cost_start

		print("Min cost: " + str(min_cost) + " latency: " + self.str_db(latency_for_min_cost, 12)) \

		min_latency_start = time.time()

		min_latency_r = self.get_r_min_latency()
		min_latency_c_network, min_latency_c_compute = self.get_cost_with_r(min_latency_r)
		cost_for_min_latency = min_latency_c_network + min_latency_c_compute
		network_le, compute_le = self.get_latency_with_r(min_latency_r)
		min_latency = network_le + compute_le#[0] + latency[1]

		min_latency_elapse_time = time.time() - min_latency_start

		print("Min Latency cost: " + str(cost_for_min_latency) + " latency: " + self.str_db((min_latency), 12)) 
		#set pulp variable
		max_latency_variable, reduce_task_placement_variable = self.init_variable()

		prob = LpProblem('Min query latency', LpMinimize)

		#set constraints
		self.set_constraints(prob, max_latency_variable, reduce_task_placement_variable, min_cost, cost_for_min_latency, latency_for_min_cost, min_latency)
		
		self.set_objective(prob, max_latency_variable, reduce_task_placement_variable)

		if self.use_cplex == True:
			solver = CPLEX(msg=self.msg, epgap=self.epgap)
		else:
			solver = GUROBI(msg=self.msg, epgap=self.epgap)
		status = prob.solve(solver)

		prob.writeLP('trips.lp')

		end = time.time()

		if status > 0:
			reduce_task_placement = self.get_reduce_task_placement(reduce_task_placement_variable)
			print(reduce_task_placement)

			self.print_result(reduce_task_placement, 'Cost Spot: ' + str(self.goals['desired_cost_spot']) + ' Performance Spot: ' + str(self.goals['desired_latency_spot']))
			print('Take time: ' + str(end - start) + ' ms, min cost: ' + str(min_cost_elapse_time) + ' ms, min latency: ' + str(min_latency_elapse_time) + ' ms' + '\n')

			with_estimated_latency = self.estimate_latency_shuffle(reduce_task_placement)

			return json.dumps(with_estimated_latency)		
		else:
			print('failed to determine a placement')
			return False

	def get_expected_latency(self, fraction, _to):
		max_expected_network_latency = 0
		expected_network_latency = 0
		expected_compute_latency = 0
		
		max_expected_network_latency = 0
		return max_expected_network_latency

	def estimate_latency(self, task_placement):
		with_estimated_latency = {}
		
		for _dc in self.dc_list:
			for task_index in task_placement[_dc]:
				with_estimated_latency[task_index] = {}
				expected_latency = self.get_expected_latency(task_placement, _dc)
				with_estimated_latency[task_index]['target_host'] = _dc
				with_estimated_latency[task_index]['expected_latency'] = expected_latency

		return with_estimated_latency	

	def estimate_latency_shuffle(self, task_placement):
		with_estimated_latency = {}
	
		for task_index in task_placement:
			hostname = task_placement[task_index]
			expected_latency = self.get_expected_latency(task_index, hostname)

			with_estimated_latency[task_index] = {}
			with_estimated_latency[task_index]['target_host'] = hostname
			with_estimated_latency[task_index]['expected_latency'] = expected_latency
		return with_estimated_latency	


	def print_result_map(self, task_placement_map,  task_placement_fraction_map, text):
		print('>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>')
		print('-- Found optimized task placement for map')
		for _dc in self.dc_list:
			task_placement_map[_dc] = [int(x) for x in task_placement_map[_dc] ]
		for _dc in self.dc_list:
			print(_dc + ' -> ' + str(sorted(task_placement_map[_dc])))
		total_cost_map, network_cost_map, compute_cost_map = self.get_cost_with_f(task_placement_fraction_map)
		total_latency_map, network_latency_map, compute_latency_map = self.get_latency_with_f(task_placement_fraction_map)

		print(text + ' - total_cost_map: ' + str(total_cost_map) + ' - network_cost_map: ' + str(network_cost_map) + ' - compute_cost_map: ' + str(compute_cost_map) \
			+ ' total_latency_map: ' + self.str_db(total_latency_map, 10) + ' network_latency_map: ' + self.str_db(network_latency_map, 10) + ' compute_latency_map: ' + self.str_db(compute_latency_map, 10)) \

		print ('>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>' + '\n')

	def print_result(self, task_placement, text):
		print ('-------------------------------------------------')
		print ('-- Found optimized task placement')
		self.print_reversed_task_placement(task_placement)
		print('-------------------------------------------------' + '\n')

		cost = self.get_cost(task_placement)
		latency = self.get_latency(task_placement)
		total_size, remote_size = self.get_data_size(task_placement)

	
		print(text + ' - cost: ' + self.str_db(sum(cost), 10) \
			+ ' (' + self.str_db(cost[0], 2) + ' + ' + self.str_db(cost[1], 2) + ')' \
			+ ' latency: ' + self.str_db(sum(latency), 10)) \
			+ ' (' + self.str_db(latency[0], 2) + ' + ' + self.str_db(latency[1], 2) + ')' + '\n'

	def print_reversed_task_placement(self, task_placement):
		inv_map = {}
		for k, v in task_placement.iteritems():
			inv_map[v] = inv_map.get(v, [])
			inv_map[v].append(int(k))

		for hostname in inv_map:
			print(hostname + ' -> ' + str(sorted(inv_map[hostname])))

	def get_min_cost_with_inputs(self):
		#get min cost
		prob = LpProblem('min cost', LpMinimize)

		task_placement_variable = LpVariable.dicts('task_placement', [(str(reduce_task_id), _to) for reduce_task_id in range(0, self.number_of_reducer) for _to in self.dc_list], 0, 1, LpBinary)

		#intermediate data needs to be mingraed into one dc
		for reduce_task_id in range(0, self.number_of_reducer):
			prob += lpSum(task_placement_variable[(str(reduce_task_id), _to)] for _to in self.dc_list) == 1.0, 'For each partition (reducer id: ' + str(reduce_task_id) + ')'

#		print migration_variable
		min_cost = lpSum(task_placement_variable[(str(reduce_task_id), _to)] \
						* self.data_size[_from][str(reduce_task_id)] \
						* self.getCostFromGBBased(self.cost[_from]['network_cost'][_to]) \
				for reduce_task_id in range(0, self.number_of_reducer) for _from in self.dc_list for _to in self.dc_list)
	
		prob += min_cost, 'min_cost'
		prob.writeLP('min_cost.lp')

		if self.use_cplex == True:
			solver = CPLEX(msg=self.msg, epgap=self.epgap)
		else:
			solver = GUROBI(msg=self.msg, epgap=self.epgap)

		status = prob.solve(solver)

		if status > 0:
			reduce_task_placement = self.get_reduce_task_placement(task_placement_variable)
			return reduce_task_placement
		else:
			return False

# map stage  MZ
	def get_f_min_cost_map(self):
		
		prob = LpProblem('min cost', LpMinimize)
		task_placement_fraction_map = LpVariable.dicts("task_placement_fraction_map", [(_from, _to) for _from in self.dc_list for _to in self.dc_list], 0, 1, cat='Continuous')
		
		min_cost_map = LpVariable('min cost map', 0)
		overall_latency_min_cost = LpVariable('overall latency min cost', 0)

		init_data_fraction_each_dc_map = {}
		for _dc in self.dc_list:
			init_data_fraction_each_dc_map[_dc] = self.data_size_each_dc_map[_dc] / float(self.total_data_size_map)

		prob += lpSum(task_placement_fraction_map[(_from, _to)] for _from in self.dc_list for _to in self.dc_list) < 1.0

		for _from in self.dc_list:
			prob += (init_data_fraction_each_dc_map[_from] >= lpSum(task_placement_fraction_map[(_from, _to)] for _to in self.dc_list))		

		data_transfer_cost_map = lpSum(task_placement_fraction_map[(_from, _to)] \
									* self.total_data_size_map \
									* self.cost[_from]['network_cost'][_to] \
									for _from in self.dc_list for _to in self.dc_list)
		
		for _from in self.dc_list:
			data_size_each_dc_transferred = self.data_size_each_dc_map[_from] \
												- lpSum(task_placement_fraction_map[(_from, _to)] \
														* self.total_data_size_map \
														for _to in self.dc_list) \
												+ lpSum(task_placement_fraction_map[(_f, _from)] \
														* self.total_data_size_map \
														for _f in self.dc_list)
			compute_time_map = data_size_each_dc_transferred \
										/ self.monitoring_info[_from]['compute_resources']['computing_bandwidth'] \
										/ self.monitoring_info[_from]['compute_resources']['node_cores']

			for _to in self.dc_list:
				data_transfer_time_map = task_placement_fraction_map[(_from, _to)] \
												* self.total_data_size_map \
												/ self.monitoring_info[_from]['network_bandwidth'][_to]	

				compute_cost_map = lpSum((data_transfer_time_map + compute_time_map) \
								* self.monitoring_info[_dc]['compute_resources']['node_cores'] \
								* self.cost[_dc]['compute_cost'] \
								for _dc in self.dc_list)				
				
				prob += min_cost_map >= data_transfer_cost_map + compute_cost_map
		
		prob += min_cost_map, 'min cost map' 

		if self.use_cplex == True:
			solver = CPLEX(msg=self.msg, epgap=self.epgap)
		else:
			solver = GUROBI(msg=self.msg, epgap=self.epgap)

		status = prob.solve(solver)

		if status > 0:
			return self.get_f_with_variable_map(task_placement_fraction_map)
		else:
			return False

	def get_cost_with_f_without_round(self, f):
		data_transfer_cost_map = sum(f[(_from, _to)] \
								* self.total_data_size_map \
								* self.cost[_from]['network_cost'][_to] \
								for _from in self.dc_list for _to in self.dc_list)

		overall_latency = 0
		network_latency = 0
		compute_latency = 0

		for _from in self.dc_list:
			data_size_each_dc_transferred = self.data_size_each_dc_map[_from] \
												- sum(f[(_from, _to)] \
														* self.total_data_size_map \
														for _to in self.dc_list) \
												+ sum(f[(_f, _from)] \
														* self.total_data_size_map \
														for _f in self.dc_list)
			compute_time_map = data_size_each_dc_transferred \
										/ self.monitoring_info[_from]['compute_resources']['computing_bandwidth'] \
										/ self.monitoring_info[_from]['compute_resources']['node_cores']

			for _to in self.dc_list:
				data_transfer_time_map = f[(_from, _to)] \
										* self.total_data_size_map \
										/ self.monitoring_info[_from]['network_bandwidth'][_to]	
				if overall_latency < data_transfer_time_map + compute_time_map:
					overall_latency = data_transfer_time_map + compute_time_map
					network_latency = data_transfer_time_map
					compute_latency = compute_time_map

		compute_cost_map = sum(overall_latency \
						* self.monitoring_info[_dc]['compute_resources']['node_cores'] \
						* self.cost[_dc]['compute_cost'] \
						for _dc in self.dc_list)

		total_cost = data_transfer_cost_map + compute_cost_map

		return total_cost

	def get_cost_with_f(self, f):
		data_transfer_cost_map = sum(round(f[(_from, _to)] * self.number_of_mapper) / self.number_of_mapper \
								* self.total_data_size_map \
								* self.cost[_from]['network_cost'][_to] \
								for _from in self.dc_list for _to in self.dc_list)

		overall_latency = 0
		network_latency = 0
		compute_latency = 0
		# self.data_transfer_time_each_dc_map = {}
		for _from in self.dc_list:
			data_size_each_dc_transferred = self.data_size_each_dc_map[_from] \
												- sum(round(f[(_from, _to)] * self.number_of_mapper) / self.number_of_mapper \
														* self.total_data_size_map \
														for _to in self.dc_list) \
												+ sum(round(f[(_f, _from)] * self.number_of_mapper) / self.number_of_mapper \
														* self.total_data_size_map \
														for _f in self.dc_list)
			compute_time_map = data_size_each_dc_transferred \
										/ self.monitoring_info[_from]['compute_resources']['computing_bandwidth'] \
										/ self.monitoring_info[_from]['compute_resources']['node_cores']

			for _to in self.dc_list:
				data_transfer_time_map = round(f[(_from, _to)] * self.number_of_mapper) / self.number_of_mapper \
										* self.total_data_size_map \
										/ self.monitoring_info[_from]['network_bandwidth'][_to]	
				if overall_latency < data_transfer_time_map + compute_time_map:
					overall_latency = data_transfer_time_map + compute_time_map
					network_latency = data_transfer_time_map
					compute_latency = compute_time_map

		compute_cost_map = sum(overall_latency \
						* self.monitoring_info[_dc]['compute_resources']['node_cores'] \
						* self.cost[_dc]['compute_cost'] \
						for _dc in self.dc_list)

		total_cost = data_transfer_cost_map + compute_cost_map

		return total_cost, data_transfer_cost_map, compute_cost_map

# map stage  MZ
	def get_f_min_latency_map(self):
		prob = LpProblem('min latency map', LpMinimize)

		task_placement_fraction_map = LpVariable.dicts("task_placement_fraction_map", [(_from, _to) for _from in self.dc_list for _to in self.dc_list], 0, 1, cat='Continuous')

		min_latency_map = LpVariable("min latency map", 0)

		init_data_fraction_each_dc_map = {}
		for _dc in self.dc_list:
			init_data_fraction_each_dc_map[_dc] = self.data_size_each_dc_map[_dc] / float(self.total_data_size_map)

		prob += lpSum(task_placement_fraction_map[(_from, _to)] for _from in self.dc_list for _to in self.dc_list) < 1.0

		for _from in self.dc_list:
			prob += (init_data_fraction_each_dc_map[_from] >= lpSum(task_placement_fraction_map[(_from, _to)] for _to in self.dc_list))		

		for _from in self.dc_list:
			data_size_each_dc_transferred = self.data_size_each_dc_map[_from] \
												- lpSum(task_placement_fraction_map[(_from, _to)] \
														* self.total_data_size_map \
														for _to in self.dc_list) \
												+ lpSum(task_placement_fraction_map[(_f, _from)] \
														* self.total_data_size_map \
														for _f in self.dc_list)
			compute_time_map = data_size_each_dc_transferred \
										/ self.monitoring_info[_from]['compute_resources']['computing_bandwidth'] \
										/ self.monitoring_info[_from]['compute_resources']['node_cores']

			for _to in self.dc_list:
				data_transfer_time_map = task_placement_fraction_map[(_from, _to)] \
												* self.total_data_size_map \
												/ self.monitoring_info[_from]['network_bandwidth'][_to]	
							
				prob += min_latency_map >= data_transfer_time_map + compute_time_map
			
		prob += min_latency_map, 'min latency map'

		if self.use_cplex == True:
			solver = CPLEX(msg=self.msg, epgap=self.epgap)
		else:
			solver = GUROBI(msg=self.msg, epgap=self.epgap)

		status = prob.solve(solver)

		if status > 0:
			return self.get_f_with_variable_map(task_placement_fraction_map)
		else:
			return False

	def get_f_with_variable_map(self, fraction_variable):
		f = {}
		for _from in self.dc_list:
			for _to in self.dc_list:
				f[(_from, _to)] = float(format(fraction_variable[(_from, _to)].varValue, '.16f')) 
		return f

	def get_latency_with_f_without_round(self, f):
		
		network_latency_map = 0.0
		compute_latency_map = 0.0
		overall_latency = 0

		for _from in self.dc_list:
			data_size_each_dc_transferred = self.data_size_each_dc_map[_from] \
												- sum(f[(_from, _to)] \
														* self.total_data_size_map \
														for _to in self.dc_list) \
												+ sum(f[(_f, _from)] \
														* self.total_data_size_map \
														for _f in self.dc_list)
			compute_time_map = data_size_each_dc_transferred \
										/ self.monitoring_info[_from]['compute_resources']['computing_bandwidth'] \
										/ self.monitoring_info[_from]['compute_resources']['node_cores']

			for _to in self.dc_list:
				data_transfer_time_map = f[(_from, _to)] \
										* self.total_data_size_map \
										/ self.monitoring_info[_from]['network_bandwidth'][_to]	
				if overall_latency < data_transfer_time_map + compute_time_map:
					overall_latency = data_transfer_time_map + compute_time_map
					network_latency_map = data_transfer_time_map
					compute_latency_map = compute_time_map

		return overall_latency

	def get_latency_with_f(self, f):

		network_latency_map = 0.0
		compute_latency_map = 0.0
		overall_latency = 0

		for _from in self.dc_list:
			data_size_each_dc_transferred = self.data_size_each_dc_map[_from] \
												- sum(round(f[(_from, _to)] * self.number_of_mapper) / self.number_of_mapper \
														* self.total_data_size_map \
														for _to in self.dc_list) \
												+ sum(round(f[(_f, _from)] * self.number_of_mapper) / self.number_of_mapper \
														* self.total_data_size_map \
														for _f in self.dc_list)
			compute_time_map = data_size_each_dc_transferred \
										/ self.monitoring_info[_from]['compute_resources']['computing_bandwidth'] \
										/ self.monitoring_info[_from]['compute_resources']['node_cores']

			for _to in self.dc_list:
				data_transfer_time_map = round(f[(_from, _to)] * self.number_of_mapper) / self.number_of_mapper \
										* self.total_data_size_map \
										/ self.monitoring_info[_from]['network_bandwidth'][_to]	
				if overall_latency < data_transfer_time_map + compute_time_map:
					overall_latency = data_transfer_time_map + compute_time_map
					network_latency_map = data_transfer_time_map
					compute_latency_map = compute_time_map

		return overall_latency, network_latency_map, compute_latency_map

# shuffle stage
	def get_r_min_cost(self):
		#get min cost
		prob = LpProblem('min cost', LpMinimize)
		fraction_variable = LpVariable.dicts('fraction', [dc for dc in self.dc_list], 0, 1, cat='Continuous')

		min_cost_shuffle = LpVariable('min cost shuffle', 0)

		prob += lpSum(fraction_variable[_dc] for _dc in self.dc_list) == 1

		data_transfer_cost_shuffle = lpSum(self.data_size_each_dc[_from] * fraction_variable[_to] * self.cost[_from]['network_cost'][_to] for _from in self.dc_list for _to in self.dc_list)

		for _from in self.dc_list:
			for _to in self.dc_list:
				data_transfer_time_shuffle = fraction_variable[_to] * self.data_size_each_dc[_from] \
											/ self.monitoring_info[_from]['network_bandwidth'][_to]
		
				data_size_each_dc_transferred = fraction_variable[_from] * self.total_data_size
				
				compute_time_shuffle = data_size_each_dc_transferred \
											/ self.monitoring_info[_from]['compute_resources']['computing_bandwidth'] \
											/ self.monitoring_info[_from]['compute_resources']['node_cores']
				compute_cost_shuffle = lpSum((data_transfer_time_shuffle + compute_time_shuffle) \
								* self.monitoring_info[_dc]['compute_resources']['node_cores'] \
								* self.cost[_dc]['compute_cost'] \
								for _dc in self.dc_list)
			
				prob += min_cost_shuffle >= data_transfer_cost_shuffle + compute_cost_shuffle

		prob += min_cost_shuffle, 'min cost shuffle'
		prob.writeLP('min_cost_r.lp')

		if self.use_cplex == True:
			solver = CPLEX(msg=self.msg, epgap=self.epgap)
		else:
			solver = GUROBI(msg=self.msg, epgap=self.epgap)

		status = prob.solve(solver)

		if status > 0:
			return self.get_r_with_variable(fraction_variable)
		else:
			return False
	
	def get_r_min_latency(self):
		prob = LpProblem('min latency shuffle', LpMinimize)
		
		fraction_variable = LpVariable.dicts('fraction', [_dc for _dc in self.dc_list], 0, 1, cat='Continuous')
		max_latency_variable = LpVariable("max latency", 0)

		#constraints
		prob += lpSum(fraction_variable[_dc] for _dc in self.dc_list) == 1

		for _from in self.dc_list:
			for _to in self.dc_list:
				data_transfer_time_shuffle = fraction_variable[_to] * self.data_size_each_dc[_from] \
											/ self.monitoring_info[_from]['network_bandwidth'][_to]
		
				data_size_each_dc_transferred = fraction_variable[_from] * self.total_data_size
				
				compute_time_shuffle = data_size_each_dc_transferred \
											/ self.monitoring_info[_from]['compute_resources']['computing_bandwidth'] \
											/ self.monitoring_info[_from]['compute_resources']['node_cores']			

				prob += max_latency_variable >= data_transfer_time_shuffle + compute_time_shuffle
	
		prob += max_latency_variable, 'max latency'
		prob.writeLP('min_latency_r.lp')

		if self.use_cplex == True:
			solver = CPLEX(msg=self.msg, epgap=self.epgap)
		else:
			solver = GUROBI(msg=self.msg, epgap=self.epgap)

		status = prob.solve(solver)

	def get_r_with_variable(self, fraction_variable):
		r = {}
		for dc in self.dc_list:
			r[dc] = float(format(fraction_variable[dc].varValue, '.16f'))

		return r

	def get_cost_with_r(self, r):

		data_transfer_cost_shuffle = sum(self.data_size_each_dc[_from] * r[_to] * self.cost[_from]['network_cost'][_to] for _from in self.dc_list for _to in self.dc_list)

		max_latency = 0.0
		max_network_latency = 0.0
		max_compute_latency = 0.0
		for _from in self.dc_list:
			for _to in self.dc_list:
				data_transfer_time_shuffle =  self.data_size_each_dc[_from] * r[_to] / self.monitoring_info[_from]['network_bandwidth'][_to]
				data_size_transferred = r[_from] * self.total_data_size
				compute_time_shuffle = data_size_transferred \
										/ self.monitoring_info[_from]['compute_resources']['computing_bandwidth'] \
										/ self.monitoring_info[_from]['compute_resources']['node_cores']

				if max_latency < data_transfer_time_shuffle + compute_time_shuffle:
					max_network_latency = data_transfer_time_shuffle
					max_compute_latency = compute_time_shuffle
					max_latency = data_transfer_time_shuffle + compute_time_shuffle			
		print("latency", max_latency)
		compute_cost_shuffle = sum(max_latency \
						* self.monitoring_info[_dc]['compute_resources']['node_cores'] \
						* self.cost[_dc]['compute_cost'] \
						for _dc in self.dc_list)		
		print("n cost", data_transfer_cost_shuffle)
		print("c cost", compute_cost_shuffle)

		return data_transfer_cost_shuffle, compute_cost_shuffle

	def get_latency_with_r(self, r):
		max_latency = 0.0
		max_network_latency = 0.0
		max_compute_latency = 0.0
		for _from in self.dc_list:
			for _to in self.dc_list:
				data_transfer_time_shuffle =  self.data_size_each_dc[_from] * r[_to] / self.monitoring_info[_from]['network_bandwidth'][_to]
				data_size_transferred = r[_from] * self.total_data_size
				compute_time_shuffle = data_size_transferred \
										/ self.monitoring_info[_from]['compute_resources']['computing_bandwidth'] \
										/ self.monitoring_info[_from]['compute_resources']['node_cores']

				if max_latency < data_transfer_time_shuffle + compute_time_shuffle:
					max_network_latency = data_transfer_time_shuffle
					max_compute_latency = compute_time_shuffle
					max_latency = data_transfer_time_shuffle + compute_time_shuffle			
			
		return max_network_latency, max_compute_latency
	
	def get_min_latency_with_inputs(self):
		prob = LpProblem('min query latency', LpMinimize)

		task_placement_variable = LpVariable.dicts('task_placement', [(str(reduce_task_id), _to) for reduce_task_id in range(0, self.number_of_reducer) for _to in self.dc_list], 0, 1, LpBinary)

		#intermediate data needs to be migrated into one dc
		for _reduce_task_id in range(0, self.number_of_reducer):
			prob += lpSum(task_placement_variable[(str(_reduce_task_id), _to)] for _to in self.dc_list) == 1.0, 'For each partition (reducer id: ' + str(_reduce_task_id) + ')'

		max_latency_variable = LpVariable("min largest latency", 0)

		from_to_bytes = {}
		aggregated_size = {}

		for _from in self.dc_list:
			from_to_bytes[_from] = {}

			for _to in self.dc_list:
				from_to_bytes[_from][_to] = lpSum(task_placement_variable[(str(reduce_task_id), _to)] \
							* self.data_size[_from][str(reduce_task_id)] \
							for reduce_task_id in range(0, self.number_of_reducer))
		
		for _to in self.dc_list:
			aggregated_size[_to] = lpSum(task_placement_variable[(str(reduce_task_id), _to)] \
								* self.data_size[_from][str(reduce_task_id)] \
								for reduce_task_id in range(0, self.number_of_reducer) for _from in self.dc_list)

		for _from in self.dc_list:
			for _to in self.dc_list:
				network_latency = from_to_bytes[_from][_to] / self.monitoring_info[_from]['network_bandwidth'][_to]

				available_cores = self.get_available_cores(_to)

				compute_latency = aggregated_size[_to] / (self.min_computing_bandwidth * available_cores)
				prob += max_latency_variable >= network_latency + compute_latency, _from + '->' + _to

		prob += max_latency_variable, 'min_largest_latency'
		prob.writeLP('min_lat.lp')

		if self.use_cplex == True:
			solver = CPLEX(msg=self.msg, epgap=self.epgap)
		else:
			solver = GUROBI(msg=self.msg, epgap=self.epgap)

		status = prob.solve(solver)
		
		if status > 0: 
			reduce_task_placement = self.get_reduce_task_placement(task_placement_variable)
			return  reduce_task_placement
		else:
			return False

	def get_min_network_usage_with_inputs(self):
		prob = LpProblem('min network usage', LpMinimize)

		task_placement_variable = LpVariable.dicts('task_placement', [(str(reduce_task_id), _to) for reduce_task_id in range(0, self.number_of_reducer) for _to in self.dc_list], 0, 1, LpBinary)

        #intermediate data needs to be migrated into one dc
		for reduce_task_id in range(0, self.number_of_reducer):
			prob += lpSum(task_placement_variable[(str(reduce_task_id), _to)] for _to in self.dc_list) == 1.0, 'For each partition (reducer id: ' + str(reduce_task_id) + ')'

		min_network_usage = lpSum(task_placement_variable[(str(reduce_task_id), _to)] \
								* self.data_size[_from][str(reduce_task_id)] \
								for reduce_task_id in range(0, self.number_of_reducer) for _from in self.dc_list for _to in self.dc_list)
	
		prob += min_network_usage, 'min_network_usage'
		prob.writeLP('min_network_usage.lp')

		if self.use_cplex == True:
			solver = CPLEX(msg=self.msg)
		else:
			solver = GUROBI(msg=self.msg)
		
		status = prob.solve(solver)

		if status > 0:
			reduce_task_placement = self.get_reduce_task_placement(task_placement_variable)
			return reduce_task_placement
		else:
			return False

	def get_map_task_placement(self, task_placement_fraction):

		task_each_dc_scheduled = copy.deepcopy(self.task_each_dc_map)
		for _from in self.dc_list:
			for _to in self.dc_list:
				if task_placement_fraction[(_from, _to)] > 0.000001:

					for i in range(0, int(round(task_placement_fraction[(_from, _to)] * self.number_of_mapper))):
						if len(task_each_dc_scheduled[_from]) > 0:
							moved_task = task_each_dc_scheduled[_from].pop()
							# print(moved_task)
							task_each_dc_scheduled[_to].append(moved_task)

		return task_each_dc_scheduled

	def get_reduce_task_placement(self, task_placement_variable):
		reduce_task_placement = {}
	
		for reduce_task_id in range(0, self.number_of_reducer):
			for _to in self.dc_list:
				if task_placement_variable[(str(reduce_task_id), _to)].varValue > 0.000001:
					reduce_task_placement[reduce_task_id] = _to

		return reduce_task_placement

	def get_data_size(self, task_placement):
		total_size = 0
		remote_size = 0

		for reduce_task_id in task_placement:
			_to = task_placement[reduce_task_id]

			for _from in self.dc_list:
				total_size += self.data_size[_from][str(reduce_task_id)]

				if _to != _from:	
					remote_size += self.data_size[_from][str(reduce_task_id)]

		return total_size, remote_size

	def get_cost(self, task_placement):

		cost_sum = 0
		data_transfer_cost_shuffle = 0
		compute_cost_shuffle = 0
		overall_latency = 0

		for reduce_task_id in task_placement:
			data_transfer_cost_shuffle += sum(self.data_size[_from][str(reduce_task_id)] * self.cost[_from]['network_cost'][task_placement[reduce_task_id]] for _from in self.dc_list)

		data_size_each_dc_transferred = {}
		for _dc in self.dc_list:
			data_size_each_dc_transferred[_dc] = 0
		for reduce_task_id in task_placement:
			_to = task_placement[reduce_task_id]
			data_size_each_dc_transferred[_to] += sum(self.data_size[_from][str(reduce_task_id)] for _from in self.dc_list)


		for _from in self.dc_list:
			data_transfer_time_shuffle = 0
			compute_time_shuffle = 0

			for _to in self.dc_list:
				data_received = 0
				for reduce_task_id in task_placement:
					if _to == task_placement[reduce_task_id]:
						data_received += self.data_size[_from][str(reduce_task_id)]
				
				data_transfer_time_shuffle = max(data_transfer_time_shuffle, data_received / self.monitoring_info[_from]['network_bandwidth'][_to])
					
			compute_time_shuffle = data_size_each_dc_transferred[_from] \
										/ self.monitoring_info[_from]['compute_resources']['computing_bandwidth'] \
										/ self.monitoring_info[_from]['compute_resources']['node_cores']
			if overall_latency < data_transfer_time_shuffle + compute_time_shuffle:
				overall_latency = data_transfer_time_shuffle + compute_time_shuffle
		
		compute_cost_shuffle = sum(overall_latency \
						* self.monitoring_info[_from]['compute_resources']['node_cores'] \
						* self.cost[_from]['compute_cost'] \
						for _from in self.dc_list)

		cost_sum = data_transfer_cost_shuffle + compute_cost_shuffle

		return data_transfer_cost_shuffle, compute_cost_shuffle

	def get_latency(self, task_placement):
		max_latency = 0.0
		max_network_latency = 0.0
		max_compute_latency = 0.0

		data_size_each_dc_transferred = {}
		for _dc in self.dc_list:
			data_size_each_dc_transferred[_dc] = 0
		for reduce_task_id in task_placement:
			_to = task_placement[reduce_task_id]
			data_size_each_dc_transferred[_to] += sum(self.data_size[_from][str(reduce_task_id)] for _from in self.dc_list)

		for _from in self.dc_list:
			data_transfer_time_shuffle = 0
			compute_time_shuffle = 0

			for _to in self.dc_list:
				data_received = 0
				for reduce_task_id in task_placement:
					if _to == task_placement[reduce_task_id]:
						data_received += self.data_size[_from][str(reduce_task_id)]
				
				data_transfer_time_shuffle = max(data_transfer_time_shuffle, data_received / self.monitoring_info[_from]['network_bandwidth'][_to])
					
			compute_time_shuffle = data_size_each_dc_transferred[_from] \
										/ self.monitoring_info[_from]['compute_resources']['computing_bandwidth'] \
										/ self.monitoring_info[_from]['compute_resources']['node_cores']
			if max_latency < data_transfer_time_shuffle + compute_time_shuffle:
				max_latency = data_transfer_time_shuffle + compute_time_shuffle
				max_network_latency = data_transfer_time_shuffle
				max_compute_latency = compute_time_shuffle
		return max_network_latency, max_compute_latency

	def print_all(self, title, task_placement):
		print(title + ':' + str(task_placement) + '\n' \
				+ '- r :' + str(self.get_r(task_placement)) \
				+ ' - network cost: ' + str(format(self.get_cost(task_placement)[0], '.0f')) \
				+ ' - compute cost: ' + str(format(self.get_cost(task_placement)[1], '.0f')) \
				+ ' - network: ' + str(format(self.get_latency(task_placement)[0])) \
				+ ' - compute: ' + str(format(self.get_latency(task_placement)[1]))) \

	def get_r(self, task_placement):
		total = 0.0
		dc_data = {}		
		r = {}
	
		for _to in self.dc_list:
			aggregated = 0

			for reduce_task_id in range(0, self.number_of_reducer):
				if task_placement[str(reduce_task_id)] == _to:
					for _from in self.dc_list:
						aggregated += self.data_size[_from][str(reduce_task_id)]
			dc_data[_to] = aggregated
			total += aggregated

		for dc in self.dc_list:
			r[dc] = dc_data[dc] / total

		return r

	def get_available_cores(self, hostname):
		return round(self.monitoring_info[hostname]['compute_resources']['node_cores'] \
                                        * (1 - self.monitoring_info[hostname]['compute_resources']['cpu_us']) , 4)

	def get_available_cores_map(self, hostname):
		return round(self.monitoring_info[hostname]['compute_resources']['node_cores'] \
                                        * (1 - self.monitoring_info[hostname]['compute_resources']['cpu_us']) , 4)

# map input setting MZ
	def set_inputs_map(self, inputs):
		self.cost = inputs['cost_info']
		self.goals = inputs['goals']
		self.monitoring_info = inputs['monitoring_info']
		#self.data_size = inputs['data_size_shuffle']
		self.data_size_map = inputs['data_size']
		self.dc_list = self.monitoring_info.keys()
		for i in range(0, len(self.dc_list)):
			self.dc_list[i] = str(self.dc_list[i])
		
		if len(self.dc_list) == 1:
# map task number
			self.number_of_mapper = 150
		else:
			self.number_of_mapper = sum(len(self.data_size_map[dc]) for dc in self.dc_list if dc in self.data_size_map)
		#set monitoring info as average
		for _from in self.dc_list: 
			for _to in self.dc_list:
				if _from == _to:
					self.monitoring_info[_from]['network_bandwidth'][_to] = 1e12
				else:
					if _to not in self.monitoring_info[_from]['network_bandwidth']:
						self.monitoring_info[_from]['network_bandwidth'][_to] = 5e6 #assume 5MB
					else:
						value = self.monitoring_info[_from]['network_bandwidth'][_to]
						if isinstance(value, list) == True:
							self.monitoring_info[_from]['network_bandwidth'][_to] = sum(value) / float(len(value))
						else:
							self.monitoring_info[_from]['network_bandwidth'][_to] = value / 1.0

		self.data_size_each_dc_map = {}
		self.task_each_dc_map = {}
		for _dc in self.dc_list:
			self.data_size_each_dc_map[_dc] = sum(self.data_size_map[_dc].values())
			self.task_each_dc_map[_dc] = self.data_size_map[_dc].keys()

 		self.total_data_size_map = sum(self.data_size_each_dc_map.values())

			
		for _dc in self.dc_list:
			for i in range(0, len(self.task_each_dc_map[_dc])):
				self.task_each_dc_map[_dc][i] = str(self.task_each_dc_map[_dc][i])

		#set infinite
		if 'computing_bandwidth_estimation' in self.goals and self.goals['computing_bandwidth_estimation'] == False:
			for dc in self.dc_list:
				self.min_computing_bandwidth = 1000000000000.0
				self.monitoring_info[dc]['compute_resources']['computing_bandwidth'] = 1000000000000.0
				self.monitoring_info[dc]['compute_resources']['cpu_us'] = 0.0
		else:
			print('-- Current computating resource infomation ')

			for _dc in self.dc_list:
				print(_dc + '-> Computing bandwidth: ' + str(self.monitoring_info[_dc]['compute_resources']['computing_bandwidth']) \
					+ ' Available CPU: ' + str((1 - self.monitoring_info[_dc]['compute_resources']['cpu_us']) * 100) + '% cores:' + str(self.get_available_cores_map(_dc)))
			print ('Total data size: ' + str(self.total_data_size_map))
			print ('Total number of mapper: ' + str(self.number_of_mapper))

		for _dc in self.dc_list:
			print(_dc + ' :: ' + str(self.data_size_each_dc_map[_dc]))
		print('\n')

# shuffle stage
	def set_inputs(self, inputs):
		self.cost = inputs['cost_info']
		self.goals = inputs['goals']
		self.monitoring_info = inputs['monitoring_info']
		self.data_size = inputs['data_size']
		self.dc_list = self.monitoring_info.keys()
		
		if len(self.dc_list) == 1:

			self.number_of_reducer = 200
		else:
			self.number_of_reducer = max(len(self.data_size[dc]) for dc in self.dc_list if dc in self.data_size)

		for dc in self.dc_list:
			if dc not in self.data_size:
				self.data_size[dc] = {}

				for i in range(0, self.number_of_reducer):
					self.data_size[dc][str(i)] = 0

		#match reducer number
		for dc in self.dc_list:
			for i in range(0, self.number_of_reducer):
				if str(i) not in self.data_size[dc]:
					self.data_size[dc][str(i)] = 0

		#set monitoring info 
		for _from in self.dc_list: 
			for _to in self.dc_list:
				if _from == _to:
					self.monitoring_info[_from]['network_bandwidth'][_to] = 1e12
				else:
					if _to not in self.monitoring_info[_from]['network_bandwidth']:
						self.monitoring_info[_from]['network_bandwidth'][_to] = 5e6 #assume 5MB
					else:
						value = self.monitoring_info[_from]['network_bandwidth'][_to]
						if isinstance(value, list) == True:
							self.monitoring_info[_from]['network_bandwidth'][_to] = sum(value) / float(len(value))
						else:
							self.monitoring_info[_from]['network_bandwidth'][_to] = value / 1.0

		self.data_size_each_dc = {}
		for _dc in self.dc_list:
			self.data_size_each_dc[_dc] = sum(self.data_size[_dc].values())
 		
		self.total_data_size = sum(self.data_size_each_dc.values())

		self.data_size_per_task  = {}
		for _reduce_id in range(0, self.number_of_reducer):
			self.data_size_per_task[str(_reduce_id)] = sum(self.data_size[_from][str(_reduce_id)] for _from in self.dc_list)

		if 'computing_bandwidth_estimation' in self.goals and self.goals['computing_bandwidth_estimation'] == False:
			for dc in self.dc_list:
				self.min_computing_bandwidth = 1000000000000.0
				self.monitoring_info[dc]['compute_resources']['computing_bandwidth'] = 1000000000000.0
				self.monitoring_info[dc]['compute_resources']['cpu_us'] = 0.0
		else:
			self.min_computing_bandwidth = min(self.monitoring_info[dc]['compute_resources']['computing_bandwidth'] for dc in self.dc_list)
	
			print('-- Current computating resource infomation ')

			for _dc in self.dc_list:
				print(_dc + '-> Computing bandwidth: ' + str(self.monitoring_info[_dc]['compute_resources']['computing_bandwidth']) \
					+ ' Available CPU: ' + str((1 - self.monitoring_info[_dc]['compute_resources']['cpu_us']) * 100) + '% cores:' + str(self.get_available_cores(_dc)))
			print ('Total data size: ' + str(self.total_data_size))
			print ('Total number of reducer: ' + str(self.number_of_reducer))


		for _dc in self.dc_list:
			print(_dc + ' : ' + str(self.data_size_each_dc[_dc]))
		print('\n')

	def estimated_task_cnt_using_r(self, desired_cost, desired_latency):
		prob = LpProblem('fiding r', LpMinimize)
		fraction_variable = LpVariable.dicts('fraction', [dc for dc in self.dc_list], 0, 1, cat='Continuous')
		max_latency_variable = LpVariable("max latency", 0)

        #constraints
		prob += lpSum(fraction_variable[dc] for dc in self.dc_list) == 1.0

		#peformance constrint
		for _to in self.dc_list:
			computation_latency = (fraction_variable[_to] * self.total_data_size) \
								/ (self.monitoring_info[_to]['compute_resources']['computing_bandwidth'] * self.get_available_cores(_to))

			for _from in self.dc_list:
				network_latency = fraction_variable[_to] * self.data_size_each_dc[_from] / self.monitoring_info[_from]['network_bandwidth'][_to]
				prob += max_latency_variable >= network_latency + computation_latency

		prob += max_latency_variable <= desired_latency

		#set cost constraint
		prob += lpSum(fraction_variable[_to] * self.data_size_each_dc[_from] * self.getCostFromGBBased(self.cost[_from]['network_cost'][_to]) 
								for _from in self.dc_list for _to in self.dc_list) <= desired_cost

		prob += max_latency_variable, 'min_largest_latency'

		if self.use_cplex == True:
			solver = CPLEX(msg=self.msg, epgap=self.epgap)
		else:
			solver = GUROBI(msg=self.msg, epgap=self.epgap)
			
		status = prob.solve(solver)

		if status > 0:
			r = {}

			for _dc in self.dc_list:
				r[_dc] = float(format(fraction_variable[_dc].varValue, '.16f'))
			return self.get_task_with_r(r)
		else:
			return False
	
	def get_task_with_r(self, r):
		task_num = {}

		for _dc in self.dc_list:
			task_num[_dc] = math.ceil(r[_dc] * self.number_of_reducer)

		return task_num

	def load_test_inputs(self):
		inputs = {}
		
		with open('./data/cost_info') as data_file:
			inputs['cost_info'] = json.load(data_file)

		with open('./data/goals') as data_file:
			inputs['goals'] = json.load(data_file)

		with open('./data/monitoring_info') as data_file:
			inputs['monitoring_info'] = json.load(data_file)

		return inputs

if __name__ == "__main__":
	dataplacement_server = TripS()

	if len(sys.argv) == 1:
		dataplacement_server.run_server(55511)
	else:
		
		inputs = {}

		with open(sys.argv[1]) as data_file:
			inputs = json.load(data_file)

		if len(sys.argv) >= 3:

			# min_cost, min_cost_latency, max_cost, max_cost_latency =  dataplacement_server.get_cost_boundary(inputs)
			# print(str(min_cost) + ' ' + str(min_cost_latency) + ' ' + str(max_cost) + ' ' + str(max_cost_latency) + '\n')

			min_cost, min_cost_without_round, min_latency_cost_map, min_latency_cost_without_round, min_cost_latency, min_cost_latency_without_round, min_latency, min_latency_without_round =  dataplacement_server.get_cost_boundary_map(inputs)
			print(str(min_cost) + ' ' + str(min_cost_without_round) + ' ' + str(min_latency_cost_map) + ' ' + str(min_latency_cost_without_round) \
			+ ' ' + str(min_cost_latency) + ' ' + str(min_cost_latency_without_round) + ' ' + str(min_latency) + ' ' + str(min_latency_without_round))
		else:
			reduce_task_placement = dataplacement_server.evaluate_map(inputs)
			# reduce_task_placement = dataplacement_server.evaluate_shuffle(inputs)

			if reduce_task_placement == False:
	#		dataplacement_server.print_all('Final decision', reduce_task_placement)
	#		print reduce_task_placement
	#		print dataplacement_server.get_size_with_r(r, inputs['dc_list'], inputs['monitoring_info'])
				print('!!!!! No possible data placement to achieve the goal in current setting')