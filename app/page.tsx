'use client'
// Define your backend API URL here
const API_URL = "https://your-backend-api.com/route";

import { useEffect, useRef, useState } from "react";

export default function MapComponent() {
  // Form state for origin, destination, and travel mode
  const [form, setForm] = useState({
    origin: "",
    destination: "",
    travelMode: "DRIVE"
  });
  const mapRef = useRef<HTMLDivElement>(null);
  const [map, setMap] = useState<google.maps.Map | null>(null);

  useEffect(() => {
    if (typeof window === "undefined") return;

    const script = document.createElement("script");
    script.src = `https://maps.googleapis.com/maps/api/js?key=${process.env.NEXT_PUBLIC_GM_API}&libraries=geometry`;
    script.async = true;
    script.onload = () => {
      if (mapRef.current) {
       const mapInstance: google.maps.Map = new google.maps.Map(mapRef.current, {
        zoom: 13,
        center: { lat: 3.139, lng: 101.6869 }, // KL
      });
      setMap(mapInstance);
      }
    };
    document.head.appendChild(script);
  }, []);

  // Submit form → call backend API
    async function handleFindRoute(e: React.FormEvent<HTMLFormElement>) {
    e.preventDefault();

    if (!map) return;

    // Clear old overlays
    map.overlayMapTypes.clear();

    const res = await fetch(API_URL, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(form)
    });

    const data = await res.json();

    // Show hazards
  data.hazards.forEach((hz: { lat: number; lng: number }) => {
      new window.google.maps.Circle({
        map,
        center: hz,
        radius: 300,
        fillColor: "red",
        strokeColor: "red",
        strokeOpacity: 0.8,
        fillOpacity: 0.35
      });
    });

    // Draw best route
    if (data.bestRoute) {
      const path = window.google.maps.geometry.encoding.decodePath(data.bestRoute.polyline);
      new window.google.maps.Polyline({
        map,
        path,
        strokeColor: "blue",
        strokeWeight: 5
      });

      // Center map on route
      const bounds = new window.google.maps.LatLngBounds();
      path.forEach(p => bounds.extend(p));
      map.fitBounds(bounds);
    }

    // Draw alternatives
    if (data.alternatives) {
      const colors = ["green", "orange", "purple"];
      data.alternatives.forEach((alt: { polyline: string }, i: number) => {
        const path = window.google.maps.geometry.encoding.decodePath(alt.polyline);
        new window.google.maps.Polyline({
          map,
          path,
          strokeColor: colors[i % colors.length],
          strokeWeight: 3,
          strokeOpacity: 0.6
        });
      });
    }
  }

  return (
    <div className="App" style={{ fontFamily: "sans-serif" }}>
      <h2>🚦 Safe Route Finder</h2>

      <form onSubmit={handleFindRoute} style={{ marginBottom: "1em" }}>
        <input
          type="text"
          placeholder="Origin"
          value={form.origin}
          onChange={e => setForm({ ...form, origin: e.target.value })}
          required
        />
        <input
          type="text"
          placeholder="Destination"
          value={form.destination}
          onChange={e => setForm({ ...form, destination: e.target.value })}
          required
        />
        <select
          value={form.travelMode}
          onChange={e => setForm({ ...form, travelMode: e.target.value })}
        >
          <option value="DRIVE">Drive</option>
          <option value="WALK">Walk</option>
          <option value="TWO-WHEELER">Two-Wheeler</option>
        </select>
        <button type="submit">Find Route</button>
      </form>

      <div ref={mapRef} style={{ width: "100%", height: "80vh" }} />
    </div>
  );
}
