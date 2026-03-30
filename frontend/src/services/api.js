const API_BASE_URL = "http://127.0.0.1:8000";

export async function getOutages({
  page = 1,
  limit = 20,
  startDate,
  endDate,
  plantId,
  plantName,
  sortBy = "period",
  sortOrder = "desc",
} = {}) {
  const params = new URLSearchParams();

  params.set("page", String(page));
  params.set("limit", String(limit));
  params.set("sort_by", sortBy);
  params.set("sort_order", sortOrder);

  if (startDate) params.set("start_date", startDate);
  if (endDate) params.set("end_date", endDate);
  if (plantId) params.set("plant_id", plantId);
  if (plantName) params.set("plant_name", plantName);

  const response = await fetch(`${API_BASE_URL}/data?${params.toString()}`);

  let payload = null;

  try {
    payload = await response.json();
  } catch {
    throw new Error("The API returned an invalid JSON response.");
  }

  if (!response.ok) {
    throw new Error(payload?.detail || "Failed to fetch outage data.");
  }

  return payload;
}

export async function refreshOutages() {
  const response = await fetch(`${API_BASE_URL}/refresh`, {
    method: "POST",
  });

  let payload = null;

  try {
    payload = await response.json();
  } catch {
    throw new Error("The API returned an invalid JSON response.");
  }

  if (!response.ok) {
    throw new Error(payload?.detail || "Failed to refresh outage data.");
  }

  return payload;
}
