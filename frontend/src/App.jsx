import { useState } from "react";
import { useAuth0 } from "@auth0/auth0-react";

function App() {

  const [apiResponse, setApiResponse] = useState("");

  const {
    isLoading,
    isAuthenticated,
    error,
    loginWithRedirect: login,
    logout: auth0Logout,
    user,
    getAccessTokenSilently,
  } = useAuth0();

  const signup = () =>
    login({ authorizationParams: { screen_hint: "signup" } });

  const logout = () =>
    auth0Logout({ logoutParams: { returnTo: window.location.origin } });

  const callProtectedApi = async () => {

    try {

      const token = await getAccessTokenSilently();

      const response = await fetch("/api/health", {
        headers: {
          Authorization: `Bearer ${token}`,
        },
      });

      const data = await response.json();

      setApiResponse(JSON.stringify(data, null, 2));

    } catch (err) {
      console.error(err);
    }
  };

  if (isLoading) return "Loading...";

  return isAuthenticated ? (
    <>
      <p>Logged in as {user.email}</p>

      <h1>User Profile</h1>

      <pre>{JSON.stringify(user, null, 2)}</pre>

      <button onClick={callProtectedApi}>
        Call Protected API
      </button>

      <pre>{apiResponse}</pre>

      <button onClick={logout}>Logout</button>
    </>
  ) : (
    <>
      {error && <p>Error: {error.message}</p>}

      <button onClick={signup}>Signup</button>

      <button onClick={login}>Login</button>
    </>
  );
}

export default App;