import React from 'react';
import { Routes, Route, Navigate } from 'react-router-dom';
import { Layout } from './components/Layout';
import { Login } from './pages/Login';
import { Dashboard } from './components/Dashboard';
import { SchemeDesigner } from './components/SchemeDesigner';
import { AdminPanel } from './components/AdminPanel';
import { KpiConfigurator } from './components/KpiConfigurator';
import { Reports } from './components/Reports';
import { SchemeExecution } from './pages/SchemeExecution';
import { useAuthStore } from './store/authStore';

function ProtectedRoute({ children }: { children: React.ReactNode }) {
  const user = useAuthStore((state) => state.user);
  
  if (!user) {
    return <Navigate to="/login" replace />;
  }
  
  return <>{children}</>;
}

function App() {
  return (
    <Routes>
      <Route path="/login" element={<Login />} />
      <Route
        path="/"
        element={
          <ProtectedRoute>
            <Layout>
              <Dashboard />
            </Layout>
          </ProtectedRoute>
        }
      />
      <Route
        path="/designer"
        element={
          <ProtectedRoute>
            <Layout>
              <SchemeDesigner />
            </Layout>
          </ProtectedRoute>
        }
      />
      <Route
        path="/admin"
        element={
          <ProtectedRoute>
            <Layout>
              <AdminPanel />
            </Layout>
          </ProtectedRoute>
        }
      />
      <Route
        path="/admin/kpi-config"
        element={
          <ProtectedRoute>
            <Layout>
              <KpiConfigurator />
            </Layout>
          </ProtectedRoute>
        }
      />
      <Route
        path="/reports"
        element={
          <ProtectedRoute>
            <Layout>
              <Reports />
            </Layout>
          </ProtectedRoute>
        }
      />
      <Route
        path="/execution"
        element={
          <ProtectedRoute>
            <Layout>
              <SchemeExecution />
            </Layout>
          </ProtectedRoute>
        }
      />
    </Routes>
  );
}

export default App