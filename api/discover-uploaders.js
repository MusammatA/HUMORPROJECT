const { createClient } = require('@supabase/supabase-js');

const SUPABASE_URL = process.env.SUPABASE_URL || 'https://secure.almostcrackd.ai';
const SERVICE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY || '';
const ANON_KEY = process.env.SUPABASE_ANON_KEY || 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InFpaHNnbmZqcW1ram1vb3d5ZmJuIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NDk1Mjc0MDAsImV4cCI6MjA2NTEwMzQwMH0.c9UQS_o2bRygKOEdnuRx7x7PeSf_OUGDtf9l3fMqMSQ';

function pickImageUrl(record = {}) {
  const candidates = [record.cdn_url, record.public_url, record.image_url, record.url];
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

function getCaptionText(row = {}) {
  const candidates = [
    row.content,
    row.caption_text,
    row.caption,
    row.text,
    row.generated_caption,
    row.meme_text,
    row.output
  ];
  for (const candidate of candidates) {
    const text = String(candidate ?? '').trim();
    if (text) return text;
  }
  return '';
}

function isValidColumbiaEmail(email) {
  return String(email || '').trim().toLowerCase().endsWith('@columbia.edu');
}

function chunkArray(items, size) {
  const out = [];
  for (let i = 0; i < items.length; i += size) out.push(items.slice(i, i + size));
  return out;
}

async function resolveUploaderIdentity(supabase, userIds) {
  const emailById = {};
  const nameById = {};
  const ids = Array.from(new Set((userIds || []).map((value) => String(value || '').trim()).filter(Boolean)));
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

  return { emailById, nameById };
}

module.exports = async function handler(req, res) {
  res.setHeader('Cache-Control', 'no-store');
  if (req.method !== 'GET') return res.status(405).json({ error: 'Method not allowed' });
  if (!SERVICE_KEY) return res.status(500).json({ error: 'Missing SUPABASE_SERVICE_ROLE_KEY env var' });

  const authHeader = String(req.headers.authorization || '');
  const token = authHeader.startsWith('Bearer ') ? authHeader.slice(7) : '';
  if (!token) return res.status(401).json({ error: 'Missing auth token' });

  try {
    const anonClient = createClient(SUPABASE_URL, ANON_KEY, { auth: { persistSession: false } });
    const serviceClient = createClient(SUPABASE_URL, SERVICE_KEY, { auth: { persistSession: false } });
    const { data: authData, error: authError } = await anonClient.auth.getUser(token);
    if (authError || !authData?.user) return res.status(401).json({ error: 'Invalid auth token' });

    const signedInEmail = String(authData.user.email || '').trim();
    if (!isValidColumbiaEmail(signedInEmail)) {
      return res.status(403).json({ error: 'A valid Columbia email is required for this directory' });
    }

    const term = String(req.query?.term || '').trim().toLowerCase();
    const rowLimit = Math.max(1000, Math.min(12000, Number(req.query?.rowLimit) || 6000));
    const pageSize = 500;
    const maxPages = Math.ceil(rowLimit / pageSize);

    let captions = [];
    for (let page = 0; page < maxPages; page += 1) {
      const from = page * pageSize;
      const to = Math.min(from + pageSize - 1, rowLimit - 1);
      const { data, error } = await serviceClient
        .from('captions')
        .select('*')
        .order('created_datetime_utc', { ascending: false })
        .range(from, to);
      if (error) throw error;
      if (!data || !data.length) break;
      captions = captions.concat(data);
      if (data.length < pageSize) break;
    }

    const imageIds = Array.from(new Set(captions.map((row) => String(row.image_id || '').trim()).filter(Boolean)));
    const imageUrlById = {};
    const imageUploaderById = {};
    for (const batch of chunkArray(imageIds, 150)) {
      const { data, error } = await serviceClient.from('images').select('*').in('id', batch);
      if (error) throw error;
      (data || []).forEach((imageRow) => {
        const id = String(imageRow.id || '').trim();
        const url = pickImageUrl(imageRow);
        if (id && url) imageUrlById[id] = url;
        if (id) imageUploaderById[id] = parseUploaderUserIdFromUrl(url || imageRow.url || '');
      });
    }

    const uploaderIds = captions
      .map((row) => {
        const explicit = String(
          row.uploader_user_id ||
          row.uploaded_by_user_id ||
          row.profile_id ||
          row.user_id ||
          row.created_by_user_id ||
          row.modified_by_user_id ||
          row.created_by ||
          ''
        ).trim();
        if (explicit) return explicit;
        return String(imageUploaderById[String(row.image_id || '').trim()] || '').trim();
      })
      .filter(Boolean);

    const { emailById, nameById } = await resolveUploaderIdentity(serviceClient, uploaderIds);

    const normalizedRows = captions
      .map((row) => {
        const imageId = String(row.image_id || '').trim();
        const imageUrl = imageUrlById[imageId] || row.image_url || row.cdn_url || row.public_url || row.url || null;
        const uploaderUserId = String(
          row.uploader_user_id ||
          row.uploaded_by_user_id ||
          row.profile_id ||
          row.user_id ||
          row.created_by_user_id ||
          row.modified_by_user_id ||
          row.created_by ||
          imageUploaderById[imageId] ||
          ''
        ).trim();
        const uploaderEmail = String(
          row.uploader_email ||
          row.uploaded_by_email ||
          row.created_by_email ||
          emailById[uploaderUserId] ||
          ''
        ).trim();
        const uploaderName = String(
          row.uploader_name ||
          row.uploaded_by_name ||
          row.created_by_name ||
          nameById[uploaderUserId] ||
          deriveNameFromEmail(uploaderEmail) ||
          ''
        ).trim();
        return {
          id: String(row.id || '').trim(),
          image_id: imageId,
          imageUrl,
          content: getCaptionText(row),
          uploaderUserId,
          uploaderEmail,
          uploaderName
        };
      })
      .filter((row) => row.imageUrl && (row.uploaderEmail || row.uploaderUserId || row.uploaderName));

    const allGroups = new Map();
    normalizedRows.forEach((row) => {
      const key = row.uploaderEmail || row.uploaderUserId || row.uploaderName;
      if (!key) return;
      const existing = allGroups.get(key) || {
        key,
        label: row.uploaderEmail || row.uploaderName || row.uploaderUserId,
        uploaderEmail: row.uploaderEmail || '',
        uploaderName: row.uploaderName || '',
        uploaderUserId: row.uploaderUserId || '',
        totalCaptions: 0,
        imageIds: new Set(),
        previewIds: new Set(),
        items: []
      };
      existing.totalCaptions += 1;
      if (row.image_id) existing.imageIds.add(row.image_id);
      const previewKey = row.image_id || row.id;
      if (previewKey && !existing.previewIds.has(previewKey) && existing.items.length < 4) {
        existing.previewIds.add(previewKey);
        existing.items.push(row);
      }
      allGroups.set(key, existing);
    });

    const groups = Array.from(allGroups.values())
      .map((group) => ({
        key: group.key,
        label: group.label,
        uploaderEmail: group.uploaderEmail,
        uploaderName: group.uploaderName,
        uploaderUserId: group.uploaderUserId,
        totalCaptions: group.totalCaptions,
        totalImages: group.imageIds.size,
        items: group.items
      }));

    const filtered = groups
      .filter((group) => {
        if (!term) return true;
        const haystack = `${group.label} ${group.uploaderEmail} ${group.uploaderName} ${group.uploaderUserId}`.toLowerCase();
        return haystack.includes(term);
      })
      .sort((a, b) => b.totalCaptions - a.totalCaptions);

    return res.status(200).json({
      totalCount: groups.length,
      uploaders: filtered
    });
  } catch (error) {
    return res.status(500).json({ error: String(error && error.message ? error.message : error) });
  }
};
