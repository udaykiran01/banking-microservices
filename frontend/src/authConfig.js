export const msalConfig = {
  auth: {
    clientId: "7cffd787-5a9b-4eb6-b214-a12769d03a7b",
    authority: "https://login.microsoftonline.com/385a89b9-c0e9-4284-804e-55f12c0b6424",
    redirectUri: "http://localhost:5173",
  },
};

export const loginRequest = {
  scopes: ["openid", "profile", "email"],
};