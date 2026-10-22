var leaderPort = "8080";
var nodes = ["8080", "8081", "8082"];

var readConsistencyMode = "leader-only-read";
const API_URL = "http://localhost:8080";

async function apiRequest(path, options = {}) {
  const response = await fetch(`${API_URL}${path}`, {
    ...options,
    headers: {
      "Content-Type": "application/json",
      ...options.headers,
    },
  });

  if (!response.ok) {
    throw new Error(
      `Request failed with status ${response.status}`
    );
  }

  if (response.status === 204) {
    return null;
  }

  return response.text();
}
async function saveEntry() {
  const key = document.getElementById("key").value.trim();
  const value = document.getElementById("value").value;

  if (!key) {
    return showResult("Please enter a name.");
  }

  try {
    await apiRequest("/kv", {
      method: "POST",
      body: JSON.stringify({ key, value }),
    });

    document.getElementById("key").value = "";
    document.getElementById("value").value = "";

    showResult("Your information has been saved successfully.");
    await renderSavedItems();
  } catch (error) {
    showResult(`Could not save information: ${error.message}`);
  }
}
async function findEntry() {
  const key = document.getElementById("searchKey").value.trim();

  if (!key) {
    return showResult("Please enter a name to search for.");
  }

  try {
    const value = await apiRequest(
      `/kv?key=${encodeURIComponent(key)}`
    );

    showResult(`Found it! Value: ${value}`);
  } catch (error) {
    showResult(`Could not find information: ${error.message}`);
  }
}
async function updateEntry(key) {
  const newValue = prompt("Enter the new value:");

  if (newValue === null) return;

  try {
    await apiRequest("/kv", {
      method: "PUT",
      body: JSON.stringify({
        key,
        value: newValue,
      }),
    });

    showResult("Your information has been updated.");
    await renderSavedItems();
  } catch (error) {
    showResult(`Could not update information: ${error.message}`);
  }
}
async function deleteEntry(key) {
  const confirmed = confirm(
    "Are you sure you want to remove this information?"
  );

  if (!confirmed) return;

  try {
    await apiRequest(
      `/kv?key=${encodeURIComponent(key)}`,
      { method: "DELETE" }
    );

    showResult("Your information has been removed.");
    await renderSavedItems();
  } catch (error) {
    showResult(`Could not remove information: ${error.message}`);
  }
}