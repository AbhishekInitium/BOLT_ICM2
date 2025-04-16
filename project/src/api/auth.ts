import { LoginFormData, User } from '../types/auth';

const MOCK_USERS = {
  'admin': { role: 'admin', clientId: 'CLIENT001' },
  'manager': { role: 'manager', clientId: 'CLIENT002' },
  'agent': { role: 'agent', clientId: 'CLIENT003' }
};

export async function authenticateUser(credentials: LoginFormData): Promise<User | null> {
  console.log('[Auth API] Attempting authentication:', {
    username: credentials.username,
    role: credentials.role,
    clientId: credentials.clientId
  });

  // Simple mock authentication
  const mockUser = MOCK_USERS[credentials.username as keyof typeof MOCK_USERS];
  
  if (mockUser && mockUser.role === credentials.role) {
    const user: User = {
      id: crypto.randomUUID(),
      username: credentials.username,
      role: credentials.role,
      clientId: credentials.clientId
    };
    return user;
  }

  return null;
}