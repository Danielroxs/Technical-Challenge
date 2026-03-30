export default function SearchForm({ value, onChange, onSubmit }) {
  return (
    // Controlled form used to apply plant name filtering from the parent component
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
