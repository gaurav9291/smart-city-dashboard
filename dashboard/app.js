if (!window.React || !window.ReactDOM || !window.Recharts) {
  document.getElementById("root").innerHTML = `
    <div class="content">
      <div class="status error">
        Dashboard scripts did not load. Check your internet connection and refresh the page.
      </div>
    </div>
  `;
  throw new Error("Missing React, ReactDOM, or Recharts browser dependency.");
}

const {
  BarChart,
  Bar,
  CartesianGrid,
  Cell,
  Legend,
  Line,
  LineChart,
  Pie,
  PieChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} = window.Recharts;

const alertColors = {
  "🟢 ALL CLEAR": "#16a34a",
  "🔴 POLLUTION TRAP": "#dc2626",
  "⚠️ ROAD CLOSED": "#d97706",
};

const zoneColors = {
  "FC Road": "#2563eb",
  Hadapsar: "#dc2626",
  Hinjewadi: "#0f9f8f",
  Kothrud: "#16a34a",
  Shivajinagar: "#d97706",
};

function formatNumber(value, suffix = "") {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return "—";
  }
  return `${Number(value).toFixed(1).replace(/\.0$/, "")}${suffix}`;
}

function formatTime(value) {
  if (!value) return "—";
  return new Date(value).toLocaleTimeString([], {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });
}

function badgeClass(record) {
  if (record.city_alert && record.city_alert.includes("POLLUTION")) return "badge red";
  if (record.city_alert && record.city_alert.includes("ROAD")) return "badge amber";
  return "badge green";
}

function Metric({ label, value, detail }) {
  return (
    <section className="metric">
      <span>{label}</span>
      <strong>{value}</strong>
      <small>{detail}</small>
    </section>
  );
}

function Panel({ title, children, wide = false }) {
  return (
    <section className={wide ? "panel wide" : "panel"}>
      <h2>{title}</h2>
      {children}
    </section>
  );
}

function App() {
  const [range, setRange] = React.useState(1440);
  const [latest, setLatest] = React.useState([]);
  const [summary, setSummary] = React.useState(null);
  const [trends, setTrends] = React.useState([]);
  const [zoneAnalytics, setZoneAnalytics] = React.useState([]);
  const [statusBreakdown, setStatusBreakdown] = React.useState({});
  const [status, setStatus] = React.useState("Loading dashboard data...");
  const [error, setError] = React.useState("");

  async function loadData() {
    try {
      setError("");
      const [
        latestResponse,
        summaryResponse,
        trendsResponse,
        zoneAnalyticsResponse,
        statusBreakdownResponse,
      ] = await Promise.all([
        fetch(`/api/latest?range_minutes=${range}`),
        fetch(`/api/summary?range_minutes=${range}`),
        fetch(`/api/trends?range_minutes=${range}`),
        fetch(`/api/zone-analytics?range_minutes=${range}`),
        fetch(`/api/status-breakdown?range_minutes=${range}`),
      ]);

      if (
        !latestResponse.ok ||
        !summaryResponse.ok ||
        !trendsResponse.ok ||
        !zoneAnalyticsResponse.ok ||
        !statusBreakdownResponse.ok
      ) {
        throw new Error("One or more dashboard API calls failed.");
      }

      const latestPayload = await latestResponse.json();
      const summaryPayload = await summaryResponse.json();
      const trendsPayload = await trendsResponse.json();
      const zoneAnalyticsPayload = await zoneAnalyticsResponse.json();
      const statusBreakdownPayload = await statusBreakdownResponse.json();

      setLatest(latestPayload.records || []);
      setSummary(summaryPayload);
      setTrends(trendsPayload.records || []);
      setZoneAnalytics(zoneAnalyticsPayload.zones || []);
      setStatusBreakdown(statusBreakdownPayload || {});
      setStatus(`Updated ${new Date().toLocaleTimeString()}`);
    } catch (err) {
      setError(err.message);
      setStatus("Dashboard data unavailable");
    }
  }

  React.useEffect(() => {
    loadData();
    const timer = setInterval(loadData, 10000);
    return () => clearInterval(timer);
  }, [range]);

  const alertData = statusBreakdown.alerts || [];
  const aqiStatusData = statusBreakdown.aqi_statuses || [];

  const trendData = trends.map((record) => ({
    time: formatTime(record.time),
    avg_aqi: record.avg_aqi,
    avg_speed: record.avg_speed,
    avg_congestion: record.avg_congestion,
    avg_temperature: record.avg_temperature,
    pollution_trap_count: record.pollution_trap_count,
  }));

  return (
    <main className="shell">
      <header className="topbar">
        <div className="brand">
          <h1>Smart City Operations</h1>
          <span>Live AQI, traffic, and weather intelligence from Elasticsearch</span>
        </div>
        <div className="controls">
          <select value={range} onChange={(event) => setRange(Number(event.target.value))}>
            <option value={15}>Last 15 minutes</option>
            <option value={30}>Last 30 minutes</option>
            <option value={60}>Last 1 hour</option>
            <option value={180}>Last 3 hours</option>
            <option value={720}>Last 12 hours</option>
            <option value={1440}>Last 24 hours</option>
          </select>
          <button onClick={loadData}>Refresh</button>
        </div>
      </header>

      <div className="content">
        <div className={error ? "status error" : "status"}>
          <span>{error || status}</span>
          <span>{latest.length} zones online</span>
        </div>

        <section className="metrics">
          <Metric label="Average AQI" value={formatNumber(summary?.avg_aqi)} detail="Latest record per zone" />
          <Metric label="Max AQI" value={formatNumber(summary?.max_aqi)} detail={`Across ${summary?.record_count || 0} records`} />
          <Metric label="Avg Congestion" value={formatNumber(summary?.avg_congestion, "%")} detail="Selected time range" />
          <Metric label="Pollution Traps" value={summary?.pollution_trap_count ?? "—"} detail={`${summary?.zone_count || 0} zones observed`} />
        </section>

        <section className="zone-list">
          {latest.map((record) => (
            <article className="zone-card" key={record.zone}>
              <h3>{record.zone}</h3>
              <span className={badgeClass(record)}>{record.city_alert}</span>
              <div className="zone-stats">
                <div><span>AQI</span><strong>{record.aqi} · {record.aqi_status}</strong></div>
                <div><span>Congestion</span><strong>{formatNumber(record.congestion_pct, "%")}</strong></div>
                <div><span>Speed</span><strong>{formatNumber(record.current_speed_kmph, " km/h")}</strong></div>
                <div><span>Temp</span><strong>{formatNumber(record.temperature_c, "°C")}</strong></div>
              </div>
            </article>
          ))}
        </section>

        <section className="grid">
          <Panel title="Average AQI by Zone">
            <div className="chart">
              <ResponsiveContainer>
                <BarChart data={zoneAnalytics}>
                  <CartesianGrid strokeDasharray="3 3" />
                  <XAxis dataKey="zone" />
                  <YAxis />
                  <Tooltip />
                  <Bar dataKey="avg_aqi" name="Average AQI">
                    {zoneAnalytics.map((entry) => (
                      <Cell key={entry.zone} fill={zoneColors[entry.zone] || "#2563eb"} />
                    ))}
                  </Bar>
                  <Bar dataKey="max_aqi" name="Max AQI" fill="#dc2626" />
                </BarChart>
              </ResponsiveContainer>
            </div>
          </Panel>

          <Panel title="Average Congestion by Zone">
            <div className="chart">
              <ResponsiveContainer>
                <BarChart data={zoneAnalytics}>
                  <CartesianGrid strokeDasharray="3 3" />
                  <XAxis dataKey="zone" />
                  <YAxis />
                  <Tooltip />
                  <Bar dataKey="avg_congestion" name="Average Congestion %" fill="#d97706" />
                  <Bar dataKey="max_congestion" name="Max Congestion %" fill="#dc2626" />
                </BarChart>
              </ResponsiveContainer>
            </div>
          </Panel>

          <Panel title="Aggregated Trend">
            <div className="chart">
              <ResponsiveContainer>
                <LineChart data={trendData}>
                  <CartesianGrid strokeDasharray="3 3" />
                  <XAxis dataKey="time" minTickGap={28} />
                  <YAxis />
                  <Tooltip />
                  <Legend />
                  <Line dataKey="avg_aqi" dot={false} name="Avg AQI" stroke="#2563eb" strokeWidth={2} type="monotone" />
                  <Line dataKey="avg_congestion" dot={false} name="Avg Congestion" stroke="#d97706" strokeWidth={2} type="monotone" />
                  <Line dataKey="avg_temperature" dot={false} name="Avg Temp" stroke="#0f9f8f" strokeWidth={2} type="monotone" />
                </LineChart>
              </ResponsiveContainer>
            </div>
          </Panel>

          <Panel title="Alert Distribution">
            <div className="chart">
              <ResponsiveContainer>
                <PieChart>
                  <Tooltip />
                  <Legend />
                  <Pie data={alertData} dataKey="value" nameKey="name" outerRadius={100}>
                    {alertData.map((entry) => (
                      <Cell key={entry.name} fill={alertColors[entry.name] || "#2563eb"} />
                    ))}
                  </Pie>
                </PieChart>
              </ResponsiveContainer>
            </div>
          </Panel>

          <Panel title="AQI Status Distribution">
            <div className="chart">
              <ResponsiveContainer>
                <PieChart>
                  <Tooltip />
                  <Legend />
                  <Pie data={aqiStatusData} dataKey="value" nameKey="name" outerRadius={100}>
                    {aqiStatusData.map((entry) => (
                      <Cell key={entry.name} fill={entry.name === "Hazardous" ? "#dc2626" : entry.name === "Severe" ? "#d97706" : "#2563eb"} />
                    ))}
                  </Pie>
                </PieChart>
              </ResponsiveContainer>
            </div>
          </Panel>

          <Panel title="Zone Analytics Summary">
            <div className="table-wrap">
              <table>
                <thead>
                  <tr>
                    <th>Zone</th>
                    <th>Records</th>
                    <th>Avg AQI</th>
                    <th>Max AQI</th>
                    <th>Avg Congestion</th>
                    <th>Max Congestion</th>
                    <th>Avg Speed</th>
                    <th>Trap Events</th>
                  </tr>
                </thead>
                <tbody>
                  {zoneAnalytics.map((record) => (
                    <tr key={record.zone}>
                      <td>{record.zone}</td>
                      <td>{record.record_count}</td>
                      <td>{formatNumber(record.avg_aqi)}</td>
                      <td>{formatNumber(record.max_aqi)}</td>
                      <td>{formatNumber(record.avg_congestion, "%")}</td>
                      <td>{formatNumber(record.max_congestion, "%")}</td>
                      <td>{formatNumber(record.avg_speed, " km/h")}</td>
                      <td>{record.pollution_trap_count}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </Panel>

          <Panel title="Latest Records" wide>
            <div className="table-wrap">
              <table>
                <thead>
                  <tr>
                    <th>Zone</th>
                    <th>AQI</th>
                    <th>AQI Status</th>
                    <th>Congestion</th>
                    <th>Traffic</th>
                    <th>Speed</th>
                    <th>Temp</th>
                    <th>Humidity</th>
                    <th>Weather</th>
                    <th>Alert</th>
                    <th>Updated</th>
                  </tr>
                </thead>
                <tbody>
                  {latest.map((record) => (
                    <tr key={record.zone}>
                      <td>{record.zone}</td>
                      <td>{record.aqi}</td>
                      <td>{record.aqi_status}</td>
                      <td>{formatNumber(record.congestion_pct, "%")}</td>
                      <td>{record.traffic_status}</td>
                      <td>{formatNumber(record.current_speed_kmph, " km/h")}</td>
                      <td>{formatNumber(record.temperature_c, "°C")}</td>
                      <td>{formatNumber(record.humidity_pct, "%")}</td>
                      <td>{record.weather_condition}</td>
                      <td>{record.city_alert}</td>
                      <td>{formatTime(record.synchronized_time)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </Panel>
        </section>
      </div>
    </main>
  );
}

ReactDOM.createRoot(document.getElementById("root")).render(<App />);
