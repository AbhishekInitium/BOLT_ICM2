export type Role = 'admin' | 'manager' | 'agent';

export interface User {
  id: string;
  username: string;
  role: Role;
  clientId: string;
}

export interface LoginFormData {
  username: string;
  role: Role;
  clientId: string;
}