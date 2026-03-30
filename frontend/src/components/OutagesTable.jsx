export default function OutagesTable({ items }) {
  return (
    <div className="table-wrapper">
      <table>
        <thead>
          <tr>
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
            <tr key={item.outage_id}>
              <td>
                <div>{item.plant_name}</div>
                <small>{item.plant_id}</small>
              </td>
              <td>{item.period}</td>
              <td>{item.capacity_mw}</td>
              <td>{item.outage_mw}</td>
              <td>{item.percent_outage}</td>
              <td>{item.run_id}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
