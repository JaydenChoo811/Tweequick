'use client';

import React, { useMemo, useRef, useState } from 'react';
import { GoogleMap, Marker, Polyline, Circle, useJsApiLoader } from '@react-google-maps/api';
import polyline from '@mapbox/polyline';

const containerStyle = { width: '100%', height: '480px' };

export default function RouteMap() {
  const { isLoaded } = useJsApiLoader({
    googleMapsApiKey: process.env.NEXT_PUBLIC_GM_API|| '<YOUR_API_KEY>',
  });
  const mapRef = useRef<google.maps.Map|null>(null);

  // Get initial values from URL parameters or use defaults
  const getInitialValues = () => {
    if (typeof window !== 'undefined') {
      const urlParams = new URLSearchParams(window.location.search);
      return {
        origin: urlParams.get('origin') || 'KLCC',
        destination: urlParams.get('destination') || 'Bukit Bintang'
      };
    }
    return { origin: 'KLCC', destination: 'Bukit Bintang' };
  };

  const initialValues = getInitialValues();

  // Input states
  const [originInput, setOriginInput] = useState<string>(initialValues.origin);
  const [destinationInput, setDestinationInput] = useState<string>(initialValues.destination);
  const [isLoading, setIsLoading] = useState<boolean>(false);

  // Route data states
  const [encoded, setEncoded] = useState<string | null>(null);
  const [routeKey, setRouteKey] = useState<number>(0); // Add route key to force re-render
  interface Hazard {
    id: string | number;
    lat: number;
    lng: number;
    risk_level?: string;
  }
  
  const [hazards, setHazards] = useState<Hazard[]>([]);

  // Helper function to get hazard circle properties based on risk level
  const getHazardCircleProps = (riskLevel: string = 'Low') => {
    // Risk levels: Low, Moderate, High, Critical
    // Radius and color based on risk level
    const riskConfig = {
      'Low': {
        radius: 1500,
        fillColor: '#4caf50', // Green
        strokeColor: '#2e7d32'
      },
      'Moderate': {
        radius: 3000,
        fillColor: '#ff9800', // Orange
        strokeColor: '#e65100'
      },
      'High': {
        radius: 6000,
        fillColor: '#f44336', // Red
        strokeColor: '#b71c1c'
      },
      'Critical': {
        radius: 10000,
        fillColor: '#9c27b0', // Purple
        strokeColor: '#4a148c'
      }
    };
    
    // Normalize the risk level (case-insensitive) and default to 'Low' if not found
    const normalizedRiskLevel = Object.keys(riskConfig).find(
      key => key.toLowerCase() === riskLevel.toLowerCase()
    ) as keyof typeof riskConfig || 'Low';
    
    const config = riskConfig[normalizedRiskLevel];
    
    return {
      radius: config.radius,
      fillColor: config.fillColor,
      fillOpacity: 0.15, // Much lower opacity to prevent stacking issues
      strokeColor: config.fillColor,
      strokeOpacity: 0.8,
      strokeWeight: 2,
      // Add CSS blend mode to handle overlaps better
      zIndex: 1,
    };
  };

  // Function to fetch route data
  const fetchRoute = async (origin: string, destination: string) => {
    setIsLoading(true);
    
    // Clear existing route and hazard data before fetching new ones
    setEncoded(null);
    setHazards([]);
    setRouteKey(prev => prev + 1); // Force re-render of all circles
    
    try {
      const url = `https://498c6yefuj.execute-api.us-east-1.amazonaws.com/default/Polyline?origin=${encodeURIComponent(origin)}&destination=${encodeURIComponent(destination)}`;
      const res = await fetch(url);
      const data = await res.json();
      setEncoded(data.bestRoute?.polyline || null);
      setHazards(data.hazards || []);
    } catch (error) {
      console.error('Error fetching route:', error);
      // Ensure data is cleared even on error
      setEncoded(null);
      setHazards([]);
    } finally {
      setIsLoading(false);
    }
  };

  // Removed automatic useEffect - route only fetches when button is pressed
  // But fetch once on mount if URL parameters are present
  React.useEffect(() => {
    if (typeof window !== 'undefined') {
      const urlParams = new URLSearchParams(window.location.search);
      const urlOrigin = urlParams.get('origin');
      const urlDestination = urlParams.get('destination');
      
      // Only fetch if we have URL parameters (meaning user clicked "Get Route")
      if (urlOrigin && urlDestination) {
        fetchRoute(urlOrigin, urlDestination);
      }
    }
  }, []); // Only run once on mount

  // Decode to path usable by <Polyline>
  const path = useMemo(() => {
    if (!encoded) return [];
    // polyline.decode returns [ [lat, lng], ... ]
    return polyline.decode(encoded).map(([lat, lng]: [number, number]) => ({ lat, lng }));
  }, [encoded]);

  // Fit map bounds to path
  const onLoad = (map: google.maps.Map) => {
    mapRef.current = map;
    if (path.length > 1) {
      const bounds = new google.maps.LatLngBounds();
      path.forEach((p: { lat: number; lng: number }) => bounds.extend(p));
      map.fitBounds(bounds);
    }
  };

  if (!isLoaded) return null;

  const origin = path[0];
  const destination = path[path.length - 1];

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    
    // Reload the page with new parameters to ensure blank canvas
    const currentUrl = new URL(window.location.href);
    currentUrl.searchParams.set('origin', originInput);
    currentUrl.searchParams.set('destination', destinationInput);
    window.location.href = currentUrl.toString();
  };

  return (
    <div>
      {/* Input Form */}
      <div style={{ padding: '20px', backgroundColor: '#f5f5f5', marginBottom: '20px' }}>
        <h2 style={{ margin: '0 0 20px 0' }}>Tweequick</h2>
        <form onSubmit={handleSubmit} style={{ display: 'flex', gap: '10px', alignItems: 'center' }}>
          <div>
            <label htmlFor="origin" style={{ display: 'block', marginBottom: '5px', fontWeight: 'bold' }}>
              Origin:
            </label>
            <input
              id="origin"
              type="text"
              value={originInput}
              onChange={(e) => setOriginInput(e.target.value)}
              placeholder="Enter origin (e.g., KLCC)"
              style={{
                padding: '8px 12px',
                border: '1px solid #ccc',
                borderRadius: '4px',
                fontSize: '14px',
                width: '200px'
              }}
            />
          </div>
          
          <div>
            <label htmlFor="destination" style={{ display: 'block', marginBottom: '5px', fontWeight: 'bold' }}>
              Destination:
            </label>
            <input
              id="destination"
              type="text"
              value={destinationInput}
              onChange={(e) => setDestinationInput(e.target.value)}
              placeholder="Enter destination (e.g., Bukit Bintang)"
              style={{
                padding: '8px 12px',
                border: '1px solid #ccc',
                borderRadius: '4px',
                fontSize: '14px',
                width: '200px'
              }}
            />
          </div>
          
          <button
            type="submit"
            disabled={isLoading || !originInput.trim() || !destinationInput.trim()}
            style={{
              padding: '8px 16px',
              backgroundColor: isLoading ? '#ccc' : '#007bff',
              color: 'white',
              border: 'none',
              borderRadius: '4px',
              cursor: isLoading ? 'not-allowed' : 'pointer',
              fontSize: '14px',
              marginTop: '20px'
            }}
          >
            {isLoading ? 'Finding Route...' : 'Get Route'}
          </button>
        </form>
      </div>

      {/* Map */}
      <GoogleMap onLoad={onLoad} mapContainerStyle={containerStyle} center={origin || { lat: 3.139, lng: 101.686 }} zoom={13}>
      {origin && <Marker position={origin} label="A" />}
      {destination && <Marker position={destination} label="B" />}
      {path.length > 1 && (
        <Polyline
          path={path}
          options={{ strokeColor: '#1E90FF', strokeOpacity: 0.9, strokeWeight: 4 }}
        />
      )}

      {/* Render hazards as circles with radius based on risk level */}
      {hazards
        .sort((a, b) => {
          // Sort by risk level priority: Critical > High > Moderate > Low
          const riskOrder = { 'Critical': 4, 'High': 3, 'Moderate': 2, 'Low': 1 };
          const aRisk = riskOrder[a.risk_level as keyof typeof riskOrder] || 1;
          const bRisk = riskOrder[b.risk_level as keyof typeof riskOrder] || 1;
          return aRisk - bRisk; // Lower risk rendered first (underneath)
        })
        .map((h, index) => {
          const circleProps = getHazardCircleProps(h.risk_level || 'Low');
          return (
            <Circle
              key={`${routeKey}-${h.id}`} // Include routeKey to force remount
              center={{ lat: h.lat, lng: h.lng }}
              options={{
                ...circleProps,
                zIndex: index + 1, // Ensure proper layering
              }}
            />
          );
        })}
    </GoogleMap>
    </div>
  );
}
