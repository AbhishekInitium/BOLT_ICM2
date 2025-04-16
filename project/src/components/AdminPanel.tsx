import React from 'react';
import { Users, Settings, Shield, Database } from 'lucide-react';
import { useNavigate } from 'react-router-dom';

export function AdminPanel() {
  const navigate = useNavigate();

  return (
    <div className="space-y-6">
      <h1 className="text-2xl font-semibold text-gray-900">Admin Panel</h1>

      <div className="grid grid-cols-1 gap-6 sm:grid-cols-2 lg:grid-cols-2">
        <div className="bg-white overflow-hidden shadow rounded-lg divide-y divide-gray-200">
          <div className="px-4 py-5 sm:px-6">
            <div className="flex items-center">
              <Users className="h-6 w-6 text-indigo-600" />
              <h3 className="ml-3 text-lg font-medium text-gray-900">User Management</h3>
            </div>
          </div>
          <div className="px-4 py-5 sm:p-6">
            <div className="space-y-4">
              <button className="w-full text-left px-4 py-2 border border-gray-300 rounded-md text-sm font-medium text-gray-700 hover:bg-gray-50">
                Manage Users
              </button>
              <button className="w-full text-left px-4 py-2 border border-gray-300 rounded-md text-sm font-medium text-gray-700 hover:bg-gray-50">
                Role Assignments
              </button>
              <button className="w-full text-left px-4 py-2 border border-gray-300 rounded-md text-sm font-medium text-gray-700 hover:bg-gray-50">
                Access Control
              </button>
            </div>
          </div>
        </div>

        <div className="bg-white overflow-hidden shadow rounded-lg divide-y divide-gray-200">
          <div className="px-4 py-5 sm:px-6">
            <div className="flex items-center">
              <Settings className="h-6 w-6 text-indigo-600" />
              <h3 className="ml-3 text-lg font-medium text-gray-900">System Configuration</h3>
            </div>
          </div>
          <div className="px-4 py-5 sm:p-6">
            <div className="space-y-4">
              <button className="w-full text-left px-4 py-2 border border-gray-300 rounded-md text-sm font-medium text-gray-700 hover:bg-gray-50">
                General Settings
              </button>
              <button className="w-full text-left px-4 py-2 border border-gray-300 rounded-md text-sm font-medium text-gray-700 hover:bg-gray-50">
                Integration Settings
              </button>
              <button 
                onClick={() => navigate('/admin/kpi-config')}
                className="w-full text-left px-4 py-2 border border-gray-300 rounded-md text-sm font-medium text-gray-700 hover:bg-gray-50"
              >
                KPI Configuration
              </button>
            </div>
          </div>
        </div>

        <div className="bg-white overflow-hidden shadow rounded-lg divide-y divide-gray-200">
          <div className="px-4 py-5 sm:px-6">
            <div className="flex items-center">
              <Shield className="h-6 w-6 text-indigo-600" />
              <h3 className="ml-3 text-lg font-medium text-gray-900">Security</h3>
            </div>
          </div>
          <div className="px-4 py-5 sm:p-6">
            <div className="space-y-4">
              <button className="w-full text-left px-4 py-2 border border-gray-300 rounded-md text-sm font-medium text-gray-700 hover:bg-gray-50">
                Security Policies
              </button>
              <button className="w-full text-left px-4 py-2 border border-gray-300 rounded-md text-sm font-medium text-gray-700 hover:bg-gray-50">
                Authentication
              </button>
              <button className="w-full text-left px-4 py-2 border border-gray-300 rounded-md text-sm font-medium text-gray-700 hover:bg-gray-50">
                API Keys
              </button>
            </div>
          </div>
        </div>

        <div className="bg-white overflow-hidden shadow rounded-lg divide-y divide-gray-200">
          <div className="px-4 py-5 sm:px-6">
            <div className="flex items-center">
              <Database className="h-6 w-6 text-indigo-600" />
              <h3 className="ml-3 text-lg font-medium text-gray-900">Data Management</h3>
            </div>
          </div>
          <div className="px-4 py-5 sm:p-6">
            <div className="space-y-4">
              <button className="w-full text-left px-4 py-2 border border-gray-300 rounded-md text-sm font-medium text-gray-700 hover:bg-gray-50">
                Backup & Restore
              </button>
              <button className="w-full text-left px-4 py-2 border border-gray-300 rounded-md text-sm font-medium text-gray-700 hover:bg-gray-50">
                Data Import
              </button>
              <button className="w-full text-left px-4 py-2 border border-gray-300 rounded-md text-sm font-medium text-gray-700 hover:bg-gray-50">
                Data Export
              </button>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}