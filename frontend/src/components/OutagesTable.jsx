export default function OutagesTable({ items }) {
  return (
    <div className="table-wrapper">
      <table>
        <thead>
          <tr>
            {/* Main fields exposed by the backend for outage analysis */}
            <th>Plant</th>
            <th>Period</th>
            <th>Capacity MW</th>
            <th>Outage MW</th>
            <th>% Outage</th>
            <th>Run ID</th>
          </tr>
        </thead>

        <tbody>
          {items.map((item) => (
            // outage_id is used as a stable unique key for each outage record
            <tr key={item.outage_id}>
              <td>
                {/* Show plant name first and plant_id as secondary reference */}
                <div>{item.plant_name}</div>
                <small>{item.plant_id}</small>
              </td>

              {/* period represents the outage date returned by the API */}
              <td>{item.period}</td>

              {/* Numeric outage metrics already prepared by the backend */}
              <td>{item.capacity_mw}</td>
              <td>{item.outage_mw}</td>
              <td>{item.percent_outage}</td>

              {/* run_id helps trace which refresh execution produced the record */}
              <td>{item.run_id}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
