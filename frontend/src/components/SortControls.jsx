export default function SortControls({
  sortBy,
  sortOrder,
  onSortByChange,
  onSortOrderChange,
}) {
  return (
    <div className="sort-controls">
      <label className="field-group">
        <span>Sort by</span>
        <select value={sortBy} onChange={onSortByChange}>
          <option value="period">Period</option>
          <option value="plant_name">Plant name</option>
          <option value="capacity_mw">Capacity MW</option>
          <option value="outage_mw">Outage MW</option>
          <option value="percent_outage">% Outage</option>
        </select>
      </label>

      <label className="field-group">
        <span>Order</span>
        <select value={sortOrder} onChange={onSortOrderChange}>
          <option value="desc">Descending</option>
          <option value="asc">Ascending</option>
        </select>
      </label>
    </div>
  );
}
