const { createClient } = require('@supabase/supabase-js');

const SUPABASE_URL = process.env.SUPABASE_URL || 'https://secure.almostcrackd.ai';
const SERVICE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY || '';
const ANON_KEY = process.env.SUPABASE_ANON_KEY || '';

function toTitleCase(value) {
  return String(value || '')
    .split(' ')
    .filter(Boolean)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(' ');
}

function deriveNameFromEmail(email) {
  const local = String(email || '').split('@')[0] || '';
  return toTitleCase(local.replace(/[._-]+/g, ' ').trim() || email || 'Uploader');
}

function buildAuditUpdateFields(userId) {
  return {
    modified_by_user_id: userId
  };
}

module.exports = async function handler(req, res) {
  if (req.method !== 'POST') return res.status(405).json({ error: 'Method not allowed' });
  if (!SERVICE_KEY) return res.status(500).json({ error: 'Missing SUPABASE_SERVICE_ROLE_KEY env var' });
  if (!ANON_KEY) return res.status(500).json({ error: 'Missing SUPABASE_ANON_KEY env var' });

  const authHeader = String(req.headers.authorization || '');
  const token = authHeader.startsWith('Bearer ') ? authHeader.slice(7) : '';
  const imageId = String(req.body?.imageId || '').trim();
  if (!token || !imageId) return res.status(400).json({ error: 'Missing token or imageId' });

  try {
    const anonClient = createClient(SUPABASE_URL, ANON_KEY, { auth: { persistSession: false } });
    const serviceClient = createClient(SUPABASE_URL, SERVICE_KEY, { auth: { persistSession: false } });

    const { data: authData, error: authError } = await anonClient.auth.getUser(token);
    if (authError || !authData?.user) return res.status(401).json({ error: 'Invalid auth token' });

    const email = String(authData.user.email || '').trim();
    const userId = String(authData.user.id || '').trim();
    const displayName = String(
      authData.user.user_metadata?.full_name ||
      authData.user.user_metadata?.name ||
      authData.user.user_metadata?.display_name ||
      ''
    ).trim() || deriveNameFromEmail(email);
    const auditFields = buildAuditUpdateFields(userId);

    const payloads = [
      {
        ...auditFields,
        uploader_email: email,
        uploader_name: displayName,
        uploader_user_id: userId
      },
      {
        ...auditFields,
        uploaded_by_email: email,
        uploaded_by_name: displayName,
        uploaded_by_user_id: userId
      },
      { ...auditFields }
    ];

    for (const payload of payloads) {
      const { error } = await serviceClient.from('captions').update(payload).eq('image_id', imageId);
      if (!error) return res.status(200).json({ ok: true });
      const msg = String(error.message || '').toLowerCase();
      const isMissingColumn = msg.includes('column') && msg.includes('does not exist');
      if (!isMissingColumn) return res.status(500).json({ error: error.message || 'Update failed' });
    }

    return res.status(500).json({ error: 'No compatible uploader columns found on captions table' });
  } catch (error) {
    return res.status(500).json({ error: String(error && error.message ? error.message : error) });
  }
};
