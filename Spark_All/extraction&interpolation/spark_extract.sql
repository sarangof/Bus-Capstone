select Siri.ServiceDelivery.VehicleMonitoringDelivery.VehicleActivity[0].MonitoredVehicleJourney.LineRef as ROUTE_ID,
	   Siri.ServiceDelivery.VehicleMonitoringDelivery.VehicleActivity[0].MonitoredVehicleJourney.VehicleLocation.Latitude as latitude,
	   Siri.ServiceDelivery.VehicleMonitoringDelivery.VehicleActivity[0].MonitoredVehicleJourney.VehicleLocation.Longitude as longitude,
	   Siri.ServiceDelivery.VehicleMonitoringDelivery.VehicleActivity[0].RecordedAtTime as recorded_time,
	   Siri.ServiceDelivery.VehicleMonitoringDelivery.VehicleActivity[0].MonitoredVehicleJourney.VehicleRef as vehicle_id,
	   Siri.ServiceDelivery.VehicleMonitoringDelivery.VehicleActivity[0].MonitoredVehicleJourney.FramedVehicleJourneyRef.DatedVehicleJourneyRef as TRIP_ID,
	   Siri.ServiceDelivery.VehicleMonitoringDelivery.VehicleActivity[0].MonitoredVehicleJourney.FramedVehicleJourneyRef.DataFrameRef as tripdate,
	   Siri.ServiceDelivery.VehicleMonitoringDelivery.VehicleActivity[0].MonitoredVehicleJourney.JourneyPatternRef as SHAPE_ID,
	   Siri.ServiceDelivery.VehicleMonitoringDelivery.VehicleActivity[0].MonitoredVehicleJourney.MonitoredCall.StopPointRef as STOP_ID,
	   Siri.ServiceDelivery.VehicleMonitoringDelivery.VehicleActivity[0].MonitoredVehicleJourney.MonitoredCall.Extensions.Distances.DistanceFromCall as distance_stop,
	   Siri.ServiceDelivery.VehicleMonitoringDelivery.VehicleActivity[0].MonitoredVehicleJourney.MonitoredCall.Extensions.Distances.CallDistanceAlongRoute as distance_shape,
	   Siri.ServiceDelivery.VehicleMonitoringDelivery.VehicleActivity[0].MonitoredVehicleJourney.MonitoredCall.Extensions.Distances.PresentableDistance as status,
	   Siri.ServiceDelivery.VehicleMonitoringDelivery.VehicleActivity[0].MonitoredVehicleJourney.DestinationRef as destination
from bus