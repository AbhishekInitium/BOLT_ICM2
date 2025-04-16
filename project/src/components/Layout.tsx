import React from 'react';
import { Link, useNavigate } from 'react-router-dom';
import { LayoutDashboard, PenTool, Shield, BarChart, Settings, Users, Calculator, LogOut, Play } from 'lucide-react';
import { useAuthStore } from '../store/authStore';

export function Layout({ children }: { children: React.ReactNode }) {
  const navigate = useNavigate();
  const { user, logout } = useAuthStore();

  const handleLogout = () => {
    logout();
    navigate('/login');
  };

  return (
    <div className="min-h-screen bg-gray-50">
      <nav className="bg-white border-b border-gray-200">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
          <div className="flex justify-between h-16">
            <div className="flex">
              <Link to="/" className="flex items-center px-2 py-2 text-gray-900">
                <Calculator className="h-6 w-6 text-indigo-600" />
                <span className="ml-2 font-semibold text-xl">ICM Platform</span>
              </Link>
              <div className="hidden sm:ml-6 sm:flex sm:space-x-8">
                <Link 
                  to="/" 
                  className="flex items-center px-3 py-2 text-sm font-medium text-gray-900 hover:text-indigo-600"
                >
                  <LayoutDashboard className="h-5 w-5 mr-1" />
                  Dashboard
                </Link>
                <Link 
                  to="/designer" 
                  className="flex items-center px-3 py-2 text-sm font-medium text-gray-900 hover:text-indigo-600"
                >
                  <PenTool className="h-5 w-5 mr-1" />
                  Designer
                </Link>
                {user?.role === 'manager' && (
                  <Link 
                    to="/execution" 
                    className="flex items-center px-3 py-2 text-sm font-medium text-gray-900 hover:text-indigo-600"
                  >
                    <Play className="h-5 w-5 mr-1" />
                    Execution
                  </Link>
                )}
                {user?.role === 'admin' && (
                  <Link 
                    to="/admin" 
                    className="flex items-center px-3 py-2 text-sm font-medium text-gray-900 hover:text-indigo-600"
                  >
                    <Shield className="h-5 w-5 mr-1" />
                    Admin
                  </Link>
                )}
                <Link 
                  to="/reports" 
                  className="flex items-center px-3 py-2 text-sm font-medium text-gray-900 hover:text-indigo-600"
                >
                  <BarChart className="h-5 w-5 mr-1" />
                  Reports
                </Link>
              </div>
            </div>
            <div className="flex items-center space-x-4">
              <div className="flex items-center text-sm text-gray-700">
                <Users className="h-5 w-5 mr-1" />
                {user?.username} ({user?.role})
              </div>
              <button 
                onClick={handleLogout}
                className="p-2 rounded-full text-gray-500 hover:text-indigo-600"
              >
                <LogOut className="h-6 w-6" />
              </button>
            </div>
          </div>
        </div>
      </nav>
      <main className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        {children}
      </main>
    </div>
  );
}