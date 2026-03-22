const { createClient } = require('@supabase/supabase-js');

const SUPABASE_URL = process.env.SUPABASE_URL || 'https://secure.almostcrackd.ai';
const SERVICE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY || '';

function pickImageUrl(record = {}) {
  const candidates = [
    record.cdn_url,
    record.public_url,
    record.image_url,
    record.url
  ];
  for (const candidate of candidates) {
    const clean = String(candidate || '').trim();
    if (clean && /^(https?:)?\/\//i.test(clean)) return clean;
  }
  return null;
}

function parseUploaderUserIdFromUrl(url) {
  const raw = String(url || '').trim();
  if (!raw) return '';
  const uuidMatch = raw.match(/[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}/i);
  if (uuidMatch) return String(uuidMatch[0] || '').trim();
  const match = raw.match(/https?:\/\/[^/]+\/([^/?#]+)\//i);
  return match ? String(match[1] || '').trim() : '';
}

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

async function resolveUploaderIdentity(supabase, userIds) {
  const emailById = {};
  const nameById = {};
  const ids = Array.from(new Set((userIds || []).map((v) => String(v || '').trim()).filter(Boolean)));
  if (!ids.length) return { emailById, nameById };

  for (const batch of chunkArray(ids, 200)) {
    let rows = null;
    let error = null;
    ({ data: rows, error } = await supabase
      .from('profiles')
      .select('id,email,full_name,display_name,name')
      .in('id', batch));

    if (error) {
      ({ data: rows, error } = await supabase
        .from('profiles')
        .select('*')
        .in('id', batch));
    }

    if (!error && Array.isArray(rows)) {
      rows.forEach((row) => {
        const id = String(row.id || row.user_id || row.profile_id || '').trim();
        if (!id) return;
        const email = String(row.email || row.user_email || '').trim();
        const name = String(row.full_name || row.display_name || row.name || '').trim();
        if (email) emailById[id] = email;
        if (name) nameById[id] = name;
      });
    }
  }

  const unresolved = ids.filter((id) => !emailById[id]).slice(0, 180);
  for (const uid of unresolved) {
    try {
      const { data } = await supabase.auth.admin.getUserById(uid);
      const email = String(data?.user?.email || '').trim();
      if (email) emailById[uid] = email;
    } catch (_err) {
      // Best effort only.
    }
  }

  const unresolvedAfterSdk = ids.filter((id) => !emailById[id]).slice(0, 180);
  for (const uid of unresolvedAfterSdk) {
    try {
      const resp = await fetch(`${SUPABASE_URL}/auth/v1/admin/users/${encodeURIComponent(uid)}`, {
        method: 'GET',
        headers: {
          apikey: SERVICE_KEY,
          Authorization: `Bearer ${SERVICE_KEY}`
        }
      });
      if (!resp.ok) continue;
      const payload = await resp.json();
      const email = String(payload?.email || payload?.user?.email || '').trim();
      if (email) emailById[uid] = email;
    } catch (_err) {
      // Best effort only.
    }
  }

  return { emailById, nameById };
}

function chunkArray(items, size) {
  const out = [];
  for (let i = 0; i < items.length; i += size) out.push(items.slice(i, i + size));
  return out;
}

module.exports = async function handler(req, res) {
  res.setHeader('Cache-Control', 's-maxage=45, stale-while-revalidate=120');
  if (!SERVICE_KEY) {
    return res.status(500).json({ error: 'Missing SUPABASE_SERVICE_ROLE_KEY env var' });
  }

  try {
    const supabase = createClient(SUPABASE_URL, SERVICE_KEY, { auth: { persistSession: false } });
    const requested = Number(req.query?.limit);
    const limit = Number.isFinite(requested) ? Math.max(200, Math.min(10000, requested)) : 3000;

    const pageSize = 500;
    const maxPages = Math.ceil(limit / pageSize);
    let captions = [];
    for (let page = 0; page < maxPages; page += 1) {
      const from = page * pageSize;
      const to = Math.min(from + pageSize - 1, limit - 1);
      const { data, error } = await supabase
        .from('captions')
        .select('*')
        .order('created_datetime_utc', { ascending: false })
        .range(from, to);
      if (error) throw error;
      if (!data || !data.length) break;
      captions = captions.concat(data);
      if (data.length < pageSize) break;
    }

    const imageIds = Array.from(new Set(captions.map(c => String(c.image_id || '').trim()).filter(Boolean)));
    const images = {};
    const imageUploaderById = {};
    for (const batch of chunkArray(imageIds, 150)) {
      const { data, error } = await supabase
        .from('images')
        .select('*')
        .in('id', batch);
      if (error) throw error;
      (data || []).forEach((img) => {
        const id = String(img.id || '').trim();
        const url = pickImageUrl(img);
        if (id && url) images[id] = url;
        if (id) imageUploaderById[id] = parseUploaderUserIdFromUrl(url || img.url || '');
      });
    }

    const uploaderIds = captions
      .map((row) => {
        const explicit = String(
          row.uploader_user_id ||
          row.uploaded_by_user_id ||
          row.created_by_user_id ||
          row.modified_by_user_id ||
          row.profile_id ||
          row.user_id ||
          row.created_by ||
          ''
        ).trim();
        if (explicit) return explicit;
        const imageId = String(row.image_id || '').trim();
        return String(imageUploaderById[imageId] || '').trim();
      })
      .filter(Boolean);
    const { emailById, nameById } = await resolveUploaderIdentity(supabase, uploaderIds);

    captions = captions.map((row) => {
      const imageId = String(row.image_id || '').trim();
      const uploaderUserId = String(
        row.uploader_user_id ||
        row.uploaded_by_user_id ||
        row.created_by_user_id ||
        row.modified_by_user_id ||
        row.profile_id ||
        row.user_id ||
        row.created_by ||
        imageUploaderById[imageId] ||
        ''
      ).trim();
      const existingEmail = String(row.uploader_email || row.uploaded_by_email || row.created_by_email || '').trim();
      const existingName = String(row.uploader_name || row.uploaded_by_name || row.created_by_name || '').trim();
      const uploaderEmail = existingEmail || emailById[uploaderUserId] || '';
      const uploaderName = existingName || nameById[uploaderUserId] || deriveNameFromEmail(uploaderEmail);
      return {
        ...row,
        uploader_user_id: uploaderUserId || row.uploader_user_id || null,
        uploader_email: uploaderEmail || row.uploader_email || null,
        uploader_name: uploaderName || row.uploader_name || null
      };
    });

    return res.status(200).json({ captions, images });
  } catch (error) {
    return res.status(500).json({ error: String(error && error.message ? error.message : error) });
  }
};
