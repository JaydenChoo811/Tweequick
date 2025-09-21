
import React, { useMemo, useRef, useState } from 'react';
import { GoogleMap, Marker, Polyline, useJsApiLoader } from '@react-google-maps/api';
import polyline from '@mapbox/polyline';

const containerStyle = { width: '100%', height: '480px' };

export default function RouteMap() {
  const { isLoaded } = useJsApiLoader({
    googleMapsApiKey: process.env.NEXT_PUBLIC_GM_API || '<YOUR_API_KEY>',
  });
  const mapRef = useRef<google.maps.Map|null>(null);

  // Example: fetch encoded polyline from your API Gateway Lambda
  const [encoded, setEncoded] = useState<string | null>(null);
  const [hazards, setHazards] = useState<any[]>([]);
  React.useEffect(() => {
    const run = async () => {
      const url = 'https://0v53gcwoy9.execute-api.us-east-1.amazonaws.com/default/Polyline?origin=KLCC&destination=Bukit%20Bintang';
      const res = await fetch(url);
      const data = await res.json();
      setEncoded(data.polyline || null);
      setHazards(data.hazards || []);
    };
    run();
  }, []);

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

  return (
    <GoogleMap onLoad={onLoad} mapContainerStyle={containerStyle} center={origin || { lat: 3.139, lng: 101.686 }} zoom={13}>
      {origin && <Marker position={origin} label="A" />}
      {destination && <Marker position={destination} label="B" />}
      {path.length > 1 && (
        <Polyline
          path={path}
          options={{ strokeColor: '#1E90FF', strokeOpacity: 0.9, strokeWeight: 4 }}
        />
      )}

      {/* Optional: render hazards as markers */}
      {hazards.map(h => (
        <Marker key={h.id} position={{ lat: h.lat, lng: h.lng }} label="!" />
      ))}
    </GoogleMap>
  );
}
