select Siri.ServiceDelivery.VehicleMonitoringDelivery.VehicleActivity[0].MonitoredVehicleJourney.LineRef as Line,
	   Siri.ServiceDelivery.VehicleMonitoringDelivery.VehicleActivity[0].MonitoredVehicleJourney.VehicleLocation.Latitude as Latitude,
	   Siri.ServiceDelivery.VehicleMonitoringDelivery.VehicleActivity[0].MonitoredVehicleJourney.VehicleLocation.Longitude as Longitude,
	   Siri.ServiceDelivery.VehicleMonitoringDelivery.VehicleActivity[0].RecordedAtTime as RecordedAtTime,
	   Siri.ServiceDelivery.VehicleMonitoringDelivery.VehicleActivity[0].MonitoredVehicleJourney.VehicleRef as vehicleID,
	   Siri.ServiceDelivery.VehicleMonitoringDelivery.VehicleActivity[0].MonitoredVehicleJourney.FramedVehicleJourneyRef.DatedVehicleJourneyRef as Trip,
	   Siri.ServiceDelivery.VehicleMonitoringDelivery.VehicleActivity[0].MonitoredVehicleJourney.FramedVehicleJourneyRef.DataFrameRef as TripDate,
	   Siri.ServiceDelivery.VehicleMonitoringDelivery.VehicleActivity[0].MonitoredVehicleJourney.JourneyPatternRef as TripPattern,
	   Siri.ServiceDelivery.VehicleMonitoringDelivery.VehicleActivity[0].MonitoredVehicleJourney.MonitoredCall.StopPointRef as MonitoredCallRef,
	   Siri.ServiceDelivery.VehicleMonitoringDelivery.VehicleActivity[0].MonitoredVehicleJourney.MonitoredCall.Extensions.Distances.DistanceFromCall as DistFromCall,
	   Siri.ServiceDelivery.VehicleMonitoringDelivery.VehicleActivity[0].MonitoredVehicleJourney.MonitoredCall.Extensions.Distances.CallDistanceAlongRoute as CallDistAlongRoute,
	   Siri.ServiceDelivery.VehicleMonitoringDelivery.VehicleActivity[0].MonitoredVehicleJourney.MonitoredCall.Extensions.Distances.PresentableDistance as PresentableDistance
from bus
