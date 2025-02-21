import cassandra
import grpc
import station_pb2, station_pb2_grpc
from cassandra.cluster import Cluster, NoHostAvailable
from cassandra import ConsistencyLevel
import pandas as pd
from concurrent import futures

# Reference: https://www.baeldung.com/cassandra-consistency-levels

class Station(station_pb2_grpc.StationServicer):
    def __init__(self):
        self.cluster = Cluster(['p6-db-1', 'p6-db-2', 'p6-db-3'])
        self.cass = self.cluster.connect()
        self.insert_statement = self.cass.prepare("""
        INSERT INTO weather.stations (id, date, record)
        VALUES (?, ?, {tmin:?, tmax:?})
        """)
        self.insert_statement.consistency_level = ConsistencyLevel.ONE
        self.max_statement = self.cass.prepare("""
        SELECT record.tmax 
        FROM weather.stations 
        WHERE id = ?
        """)
        self.max_statement.consistency_level = ConsistencyLevel.THREE

    def RecordTemps(self, request, context):
        errorMsg = ""
        try:
            curr_id = request.station
            curr_date = request.date
            curr_tmin = request.tmin
            curr_tmax = request.tmax

            self.cass.execute(self.insert_statement, (curr_id, curr_date, curr_tmin, curr_tmax))
        except cassandra.Unavailable as e:
            print("here1")
            rr = e.required_replicas
            ar = e.alive_replicas
            errorMsg = f"need {rr} replicas, but only have {ar}"
        except cassandra.cluster.NoHostAvailable as f:
            print("here2")
            for i in f.errors:
                if type(i) == "cassandra.Unavailable":
                    rr = i.required_replicas
                    ar = i.alive_replicas
                    errorMsg = f"need {rr} replicas, but only have {ar}"
        except Exception as g:
            print("here3")
            errorMsg = string(g)
        finally:
            return station_pb2.RecordTempsReply(error = errorMsg)

    def StationMax(self, request, context):
        errorMsg = ""
        maxtemp = int(0)
        try:
            curr_station = request.station
            print("here1")
            maxtemp = int(pd.DataFrame(self.cass.execute(self.max_statement, (curr_station,)))['record_tmax'].max())
            print("here2")
        except cassandra.Unavailable as e:
            print("here3")
            rr = e.required_replicas
            ar = e.alive_replicas
            errorMsg = f"need {rr} replicas, but only have {ar}"
        except cassandra.cluster.NoHostAvailable as f:
            print("here4")
            for i in f.errors:
                if type(i) == "cassandra.Unavailable":
                    rr = i.required_replicas
                    ar = i.alive_replicas
                    errorMsg = f"need {rr} replicas, but only have {ar}"
        except Exception as g:
            print("here5")
            errorMsg = string(g)
        finally:
            print(type(maxtemp))
            print(type(errorMsg))
            print(errorMsg)
            return station_pb2.StationMaxReply(tmax = maxtemp, error = errorMsg)

if __name__=="__main__":
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=4), options=(('grpc.so_reuseport', 0),))
    station_pb2_grpc.add_StationServicer_to_server(Station(), server)
    server.add_insecure_port("[::]:5440", )
    server.start()
    server.wait_for_termination()