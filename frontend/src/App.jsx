import { useState, useEffect } from "react";
import { MapContainer, TileLayer, CircleMarker, Popup } from "react-leaflet";
import "leaflet/dist/leaflet.css";
import "./index.css";
import { FaCog } from "react-icons/fa";

export default function App() {
  const [points, setPoints] = useState([]);
  const [location, setLocation] = useState("");
  const [beginDate, setBeginDate] = useState("");
  const [endDate, setEndDate] = useState("");
  const [tier, setTier] = useState("");
  const [popupVisible, setPopupVisible] = useState(false);
  const [threshold1, setThreshold1] = useState("");
  const [threshold2, setThreshold2] = useState("");
  const [sidebarVisible, setSidebarVisible] = useState(false);

  const API_HOST = "http://localhost:8000";

  useEffect(() => {
    fetchAll();
  }, []);

  const fetchAll = async () => {
    try {
      const res = await fetch(`${API_HOST}/api/radiation/all`);
      const json = await res.json();
      if (json.status === "success") setPoints(json.data);
    } catch (err) {
      console.error("Initial fetch failed:", err);
    }
  };

  const fetchByRisk = async (risk) => {
    try {
      const res = await fetch(`${API_HOST}/api/radiation/${risk}`);
      const json = await res.json();
      if (json.status === "success") setPoints(json.data);
    } catch (err) {
      console.error(`${risk} fetch failed`, err);
    }
  };

  const fetchByDate = async () => {
    if (!beginDate || !endDate) return alert("Both dates required.");
    try {
      const res = await fetch(
        `${API_HOST}/api/radiation/by-date?begin=${beginDate}&end=${endDate}`
      );
      const json = await res.json();
      if (json.status === "success") setPoints(json.data);
    } catch (err) {
      console.error("Date filter failed:", err);
    }
  };

  const fetchByLocation = async () => {
    if (!location.trim()) return alert("Enter a location.");
    try {
      const res = await fetch(
        `${API_HOST}/api/radiation/by-location?query=${encodeURIComponent(
          location
        )}`
      );
      const json = await res.json();
      if (json.status === "success") setPoints(json.data);
    } catch (err) {
      console.error("Location filter failed:", err);
    }
  };

  const applyTierFilter = async () => {
    if (tier === "2") {
      const limit = parseFloat(threshold1);
      if (isNaN(limit)) return alert("Valid threshold required.");
      try {
        const res = await fetch(
          `${API_HOST}/api/radiation/by-threshold?tier=2&limit=${limit}`
        );
        const json = await res.json();
        if (json.status === "success") setPoints(json.data);
      } catch (err) {
        console.error("2-tier error:", err);
      }
    } else if (tier === "3") {
      const low = parseFloat(threshold1);
      const high = parseFloat(threshold2);
      if (isNaN(low) || isNaN(high) || low >= high)
        return alert("Enter valid thresholds (low < high)");
      try {
        const res = await fetch(
          `${API_HOST}/api/radiation/by-threshold?tier=3&low=${low}&high=${high}`
        );
        const json = await res.json();
        if (json.status === "success") setPoints(json.data);
      } catch (err) {
        console.error("3-tier error:", err);
      }
    }
    setPopupVisible(false);
  };

  const closeSidebar = () => {
    setSidebarVisible(false);
    setPopupVisible(false);
  };

  const getColor = (risk) => {
    if (risk === "dangerous") return "red";
    if (risk === "elevated") return "orange";
    return "green";
  };

  return (
    <div className="relative min-h-screen bg-gray-100 font-sans">
      <header className="bg-white shadow p-4 flex justify-between items-center">
        <h1 className="text-xl font-bold text-gray-800">
          🌐 Radiation Tracker
        </h1>
        <div className="flex items-center space-x-6">
          <div className="flex items-center space-x-2">
            <span className="w-3 h-3 rounded-full bg-green-500"></span>
            <span className="text-sm text-gray-600">Safe</span>
          </div>
          <div className="flex items-center space-x-2">
            <span className="w-3 h-3 rounded-full bg-orange-500"></span>
            <span className="text-sm text-gray-600">Elevated</span>
          </div>
          <div className="flex items-center space-x-2">
            <span className="w-3 h-3 rounded-full bg-red-500"></span>
            <span className="text-sm text-gray-600">Dangerous</span>
          </div>
        </div>
      </header>

      {!sidebarVisible && (
        <button
          onClick={() => setSidebarVisible(true)}
          className="fixed top-4 left-4 bg-white p-2 rounded shadow z-[1000]"
        >
          <FaCog size={20} />
        </button>
      )}

      <div className="bg-red-500">If this is red, Tailwind is working</div>

      {sidebarVisible && (
        <aside className="fixed top-0 left-0 h-full w-72 bg-white shadow-md p-4 z-[1000] space-y-4">
          <div className="flex justify-between items-center">
            <h2 className="text-lg font-semibold">Filter Settings</h2>
            <button
              onClick={closeSidebar}
              className="text-gray-500 hover:text-gray-700"
            >
              ✕
            </button>
          </div>

          <div className="flex justify-between items-center gap-1">
            <button
              onClick={() => fetchByRisk("safe")}
              className="bg-green-500 px-2.5 py-1.5 rounded text-white"
            >
              Safe
            </button>
            <button
              onClick={() => fetchByRisk("elevated")}
              className="bg-orange-500 px-2.5 py-1.5 rounded text-white"
            >
              Elevated
            </button>
            <button
              onClick={() => fetchByRisk("dangerous")}
              className="bg-red-500 px-2.5 py-1.5 rounded text-white"
            >
              Dangerous
            </button>
          </div>

          <button
            onClick={fetchAll}
            className="bg-gray-700 w-full text-white px-3 py-1.5 rounded"
          >
            All Data
          </button>

          <hr className="my-2" />

          <input
            type="text"
            placeholder="Country name"
            value={location}
            onChange={(e) => setLocation(e.target.value)}
            className="border p-2 w-full"
          />
          <button
            onClick={fetchByLocation}
            className="bg-blue-500 text-white px-4 py-2 rounded w-full"
          >
            Search Location
          </button>

          <hr className="my-2" />

          <input
            type="date"
            placeholder="Begin date"
            value={beginDate}
            onChange={(e) => setBeginDate(e.target.value)}
            className="border p-2 w-full"
          />
          <input
            type="date"
            placeholder="Begin date"
            value={endDate}
            onChange={(e) => setEndDate(e.target.value)}
            className="border p-2 w-full"
          />
          <button
            onClick={fetchByDate}
            className="bg-blue-600 text-white px-4 py-2 rounded w-full"
          >
            Filter Date
          </button>

          <hr className="my-2" />
          <select
            className="border p-2 w-full"
            value={tier}
            onChange={(e) => {
              setTier(e.target.value);
              if (e.target.value === "2" || e.target.value === "3")
                setPopupVisible(true);
            }}
          >
            <option value="">Select Tier</option>
            <option value="2">2-Tier</option>
            <option value="3">3-Tier</option>
          </select>

          <button
            onClick={closeSidebar}
            className="bg-gray-300 w-full text-gray-800 px-4 py-2 rounded mt-4"
          >
            Close
          </button>
        </aside>
      )}

      {popupVisible && (
        <div className="fixed inset-0 bg-black bg-opacity-60 flex items-center justify-center z-50">
          <div className="bg-white p-6 rounded-lg shadow-xl space-y-4">
            <h2 className="text-xl font-semibold">Set CPM Threshold</h2>
            {tier === "2" && (
              <input
                type="number"
                placeholder="Max safe CPM"
                value={threshold1}
                onChange={(e) => setThreshold1(e.target.value)}
                className="border p-2 w-full"
              />
            )}
            {tier === "3" && (
              <>
                <input
                  type="number"
                  placeholder="Safe max"
                  value={threshold1}
                  onChange={(e) => setThreshold1(e.target.value)}
                  className="border p-2 w-full"
                />
                <input
                  type="number"
                  placeholder="Elevated max"
                  value={threshold2}
                  onChange={(e) => setThreshold2(e.target.value)}
                  className="border p-2 w-full"
                />
              </>
            )}
            <button
              onClick={applyTierFilter}
              className="bg-blue-600 text-white px-4 py-2 rounded w-full"
            >
              Apply Threshold
            </button>
          </div>
        </div>
      )}

      <MapContainer center={[20, 0]} zoom={2} className="h-screen z-0">
        <TileLayer
          attribution="&copy; OpenStreetMap contributors"
          url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
        />

        {points.map((p) => (
          <CircleMarker
            key={`${p.device_id}-${p.captured_time}`}
            center={[p.latitude, p.longitude]}
            radius={8}
            pathOptions={{
              color: getColor(p.risk_level),
              fillOpacity: 0.7,
            }}
          >
            <Popup>
              <strong>Sensor:</strong> {p.device_id}
              <br />
              <strong>CPM:</strong> {p.cpm}
              <br />
              <strong>Time:</strong> {p.captured_time}
              <br />
              <strong>Risk:</strong> {p.risk_level}
            </Popup>
          </CircleMarker>
        ))}
      </MapContainer>
    </div>
  );
}
