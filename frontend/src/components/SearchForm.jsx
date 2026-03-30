export default function SearchForm({ value, onChange, onSubmit }) {
  return (
    <form className="search-form" onSubmit={onSubmit}>
      <input
        type="text"
        placeholder="Search by plant name"
        value={value}
        onChange={onChange}
      />
      <button type="submit">Search</button>
    </form>
  );
}
